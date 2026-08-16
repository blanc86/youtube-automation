"""``PlanTimeline``: the fourth stage of the ``story_video`` pipeline.

Not listed in this task's brief's own Files list (only
``tests/unit/core/test_timeline.py`` is), but this project's history is that
every stage gets at least one test driving its own ``run()`` rather than
only the pure function it wraps - see ``test_transcribe.py``'s module
docstring for the precedent and the brief line it responds to. There is no
fake provider to inject here (``PlanTimeline`` has none - see
``app/stages/plan_timeline.py``'s own module docstring), so every test below
constructs the stage directly against a real ``CasStore``.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path

import pytest

from ytauto.app.stages.plan_timeline import PlanTimeline
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash
from ytauto.core.pipeline.stage import JobContext
from ytauto.infra.cas.store import CasStore

# db_conn is defined in tests/unit/conftest.py.

_ARBITRARY_DIGEST = ContentHash("a" * 64)
_OTHER_DIGEST = ContentHash("b" * 64)

_FULL_SETTINGS: dict[str, object] = {
    "words_per_group_min": 2,
    "words_per_group_max": 8,
    "segment_seconds_min": 4.0,
    "segment_seconds_max": 8.0,
    "seed": 1,
}


@pytest.fixture()
def cas(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    return CasStore(root=tmp_path / "cas", conn=db_conn)


def _ctx(
    *,
    settings: Mapping[str, object] | None = None,
    timings_digest: ContentHash = _ARBITRARY_DIGEST,
    workdir: Path = Path("/tmp/j1"),
) -> JobContext:
    return JobContext(
        job_id="j1",
        project_id="p1",
        settings=_FULL_SETTINGS if settings is None else settings,
        inputs={
            "transcribe": (
                ArtifactRef(name="word_timings.json", kind="json", digest=timings_digest),
            )
        },
        workdir=workdir,
    )


def test_stage_identity_and_declared_settings(cas: CasStore) -> None:
    stage = PlanTimeline(cas=cas)
    assert stage.id == "plan_timeline"
    assert stage.version == 1
    assert stage.depends_on == ("transcribe",)
    assert stage.settings_keys == (
        "words_per_group_min",
        "words_per_group_max",
        "segment_seconds_min",
        "segment_seconds_max",
        "seed",
    )


def test_the_fingerprint_follows_the_upstream_word_timings_digest(cas: CasStore) -> None:
    """A re-transcribed ``word_timings.json`` (new upstream digest) must not
    be served this stage's stale cached ``timeline.json``."""
    stage = PlanTimeline(cas=cas)
    fp_a = stage.fingerprint(_ctx(timings_digest=_ARBITRARY_DIGEST))
    fp_b = stage.fingerprint(_ctx(timings_digest=_OTHER_DIGEST))
    assert fp_a != fp_b


@pytest.mark.parametrize(
    "changed_key,changed_value",
    [
        ("words_per_group_min", 3),
        ("words_per_group_max", 9),
        ("segment_seconds_min", 5.0),
        ("segment_seconds_max", 9.0),
        ("seed", 2),
    ],
)
def test_the_fingerprint_follows_every_declared_setting(
    cas: CasStore, changed_key: str, changed_value: object
) -> None:
    """All five ``settings_keys`` change the edit (per this task's brief), so
    all five must invalidate the cache - including ``seed``, which
    ``core.pipeline.timeline.plan_timeline`` does not currently consume (see
    that module's own docstring). The fingerprint is computed from
    *declared* settings, not from anything the algorithm happens to read
    today; a future revision that starts using ``seed`` must not need a
    stage version bump just to invalidate old caches."""
    stage = PlanTimeline(cas=cas)
    baseline = stage.fingerprint(_ctx())
    changed = dict(_FULL_SETTINGS)
    changed[changed_key] = changed_value
    assert stage.fingerprint(_ctx(settings=changed)) != baseline


def test_an_undeclared_setting_does_not_change_the_fingerprint(cas: CasStore) -> None:
    """Narrowing through ``settings_keys`` (via ``stage_support.project_settings``)
    is what keeps an unrelated setting - a caption colour, a voice - from
    invalidating this stage's cache."""
    stage = PlanTimeline(cas=cas)
    baseline = stage.fingerprint(_ctx())
    with_extra = dict(_FULL_SETTINGS)
    with_extra["voice"] = "en-US-GuyNeural"
    assert stage.fingerprint(_ctx(settings=with_extra)) == baseline


def test_run_reads_the_exact_shape_transcribe_writes(cas: CasStore) -> None:
    """``word_timings.json`` is a JSON array of ``[text, start_s, end_s]``
    triples - confirmed against ``transcribe.py``'s own ``run`` before
    writing this reader (see this task's report). A reader expecting objects
    instead of arrays would raise here rather than silently misreading."""
    timings_bytes = json.dumps([["Hello", 0.0, 0.4], ["world.", 0.4, 0.9]]).encode("utf-8")
    timings_digest = cas.stage_file(timings_bytes, kind="json")
    stage = PlanTimeline(cas=cas)
    ctx = _ctx(timings_digest=timings_digest)

    result = stage.run(ctx, lambda fraction, note: None)

    timeline = json.loads(cas.read_bytes(result.artifact("timeline.json").digest))
    assert [len(g["words"]) for g in timeline["groups"]] == [2]


def test_run_emits_timeline_json_as_json_dumps_asdict(cas: CasStore) -> None:
    """Pins the exact on-disk shape ``json.dumps(asdict(timeline))`` produces
    - nested ``groups``/``segments`` as arrays of objects, not of tuples -
    since Tasks 8, 10, 11 and 12 all read this shape (per this task's
    brief)."""
    timings_bytes = json.dumps([["alone", 0.0, 0.8]]).encode("utf-8")
    timings_digest = cas.stage_file(timings_bytes, kind="json")
    stage = PlanTimeline(cas=cas)
    ctx = _ctx(timings_digest=timings_digest)

    result = stage.run(ctx, lambda fraction, note: None)

    timeline_ref = result.artifact("timeline.json")
    assert timeline_ref.kind == "json"
    timeline = json.loads(cas.read_bytes(timeline_ref.digest))
    assert timeline == {
        "duration_s": pytest.approx(0.8),
        "groups": [
            {
                "start_s": pytest.approx(0.0),
                "end_s": pytest.approx(0.8),
                "words": [["alone", pytest.approx(0.0), pytest.approx(0.8)]],
            }
        ],
        "segments": [{"start_s": pytest.approx(0.0), "end_s": pytest.approx(0.8)}],
    }


def test_run_derives_audio_duration_from_the_last_words_end(cas: CasStore) -> None:
    """See ``PlanTimeline``'s class docstring: this stage reads only
    ``word_timings.json`` (per Step 9 of this task's brief), so
    ``audio_duration_s`` is the last word's own ``end_s`` rather than a true
    probed duration. Pinned here so a future change that wires in a real
    duration is a deliberate, visible edit to this test, not a silent
    behaviour change."""
    timings_bytes = json.dumps([["word", 0.0, 0.5]]).encode("utf-8")
    timings_digest = cas.stage_file(timings_bytes, kind="json")
    stage = PlanTimeline(cas=cas)
    ctx = _ctx(timings_digest=timings_digest)

    result = stage.run(ctx, lambda fraction, note: None)

    timeline = json.loads(cas.read_bytes(result.artifact("timeline.json").digest))
    assert timeline["duration_s"] == pytest.approx(0.5)
    assert timeline["segments"][-1]["end_s"] == pytest.approx(0.5)


def test_run_handles_an_empty_word_timings_array(cas: CasStore) -> None:
    """Silence is legal input - ``duration_s`` falls back to ``0.0`` rather
    than raising on an empty ``word_timings.json``."""
    timings_digest = cas.stage_file(b"[]", kind="json")
    stage = PlanTimeline(cas=cas)
    ctx = _ctx(timings_digest=timings_digest)

    result = stage.run(ctx, lambda fraction, note: None)

    timeline = json.loads(cas.read_bytes(result.artifact("timeline.json").digest))
    assert timeline["groups"] == []
    assert timeline["duration_s"] == pytest.approx(0.0)
    assert timeline["segments"] == [{"start_s": 0.0, "end_s": 0.0}]
