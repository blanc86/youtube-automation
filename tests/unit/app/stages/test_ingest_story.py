"""``IngestStory``: the first stage of the ``story_video`` pipeline.

Implemented in ``ytauto.providers.story.pasted`` rather than under
``app/stages/`` - see that module's docstring - but grouped here with the
rest of the stage suite since what is under test is stage behaviour
(``fingerprint``/``run``), not the ``StorySource`` port itself.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from pathlib import Path

import pytest

from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.pipeline.stage import JobContext
from ytauto.infra.cas.store import CasStore
from ytauto.providers.story.pasted import IngestStory, make_stage

# db_conn is defined in tests/unit/conftest.py.


@pytest.fixture()
def cas(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    return CasStore(root=tmp_path / "cas", conn=db_conn)


def _ctx(
    *,
    settings: Mapping[str, object] | None = None,
    inputs: Mapping[str, tuple[ArtifactRef, ...]] | None = None,
    workdir: Path = Path("/tmp/j1"),
) -> JobContext:
    return JobContext(
        job_id="j1",
        project_id="p1",
        settings={} if settings is None else settings,
        inputs={} if inputs is None else inputs,
        workdir=workdir,
    )


def test_stage_identity_and_declared_settings(cas: CasStore) -> None:
    stage = IngestStory(cas=cas, settings={})
    assert stage.id == "ingest_story"
    assert stage.version == 1
    assert stage.depends_on == ()
    assert stage.settings_keys == ("story_digest",)


def test_the_stage_fingerprint_follows_the_story_digest_not_the_path(cas: CasStore) -> None:
    stage = IngestStory(cas=cas, settings={})
    fp_a = stage.fingerprint(_ctx(settings={"story_digest": "a" * 64, "story_path": "/x"}))
    fp_b = stage.fingerprint(_ctx(settings={"story_digest": "a" * 64, "story_path": "/y"}))
    fp_c = stage.fingerprint(_ctx(settings={"story_digest": "b" * 64, "story_path": "/x"}))
    assert fp_a == fp_b, "the path must not reach the fingerprint"
    assert fp_a != fp_c, "the digest must reach the fingerprint"


def test_run_emits_story_txt_with_the_fetched_bytes(tmp_path: Path, cas: CasStore) -> None:
    story_path = tmp_path / "story.txt"
    story_path.write_text("Once upon a time.\n", encoding="utf-8")
    stage = IngestStory(cas=cas, settings={})
    ctx = _ctx(settings={"story_path": str(story_path), "story_digest": "a" * 64})

    result = stage.run(ctx, lambda fraction, note: None)

    artifact = result.artifact("story.txt")
    assert artifact.kind == "text"
    assert cas.read_bytes(artifact.digest) == b"Once upon a time.\n"


def test_run_propagates_a_fatal_provider_error_for_a_missing_story(
    tmp_path: Path, cas: CasStore
) -> None:
    """``run`` does not swallow ``PastedStorySource.fetch``'s error - it is
    ``run_stage`` (the worker's caller), not the stage, that translates it
    into a worker-protocol message."""
    stage = IngestStory(cas=cas, settings={})
    ctx = _ctx(settings={"story_path": str(tmp_path / "absent.txt"), "story_digest": "a" * 64})

    with pytest.raises(ProviderError) as exc:
        stage.run(ctx, lambda fraction, note: None)
    assert exc.value.kind is ErrorKind.FATAL


def test_make_stage_returns_an_ingest_story_that_writes_through_its_cas(
    tmp_path: Path, cas: CasStore
) -> None:
    story_path = tmp_path / "story.txt"
    story_path.write_text("hello\n", encoding="utf-8")
    stage = make_stage(cas=cas, settings={"voice": "en-GB-RyanNeural"})
    assert isinstance(stage, IngestStory)
    ctx = _ctx(settings={"story_path": str(story_path), "story_digest": "a" * 64})

    result = stage.run(ctx, lambda fraction, note: None)

    assert cas.read_bytes(result.artifact("story.txt").digest) == b"hello\n"
