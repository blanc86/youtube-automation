"""``IngestStory``: the first stage of the ``story_video`` pipeline.

Typed against ``core.ports.providers.StorySource``, never against a concrete
provider class - ``ytauto.app`` may not import ``ytauto.providers`` (an
import-linter ``forbidden`` contract). Most tests here still exercise
``IngestStory`` with a real ``PastedStorySource`` injected, because that is
the realistic path; ``test_run_reads_through_the_injected_storysource_not_a_hardcoded_one``
below is the one that matters structurally - it is the proof a fake
``StorySource`` can stand in at all, which is the entire reason
``IngestStory`` depends on the Protocol rather than the concrete class.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from pathlib import Path

import pytest

from ytauto.app.stages.ingest_story import IngestStory
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.pipeline.stage import JobContext
from ytauto.core.ports.capability import CapabilityDescriptor, CostModel, LatencyClass
from ytauto.infra.cas.store import CasStore
from ytauto.providers.story.pasted import PastedStorySource

# db_conn is defined in tests/unit/conftest.py.


class _FakeStorySource:
    """A ``StorySource`` double that never touches the filesystem.

    That ``IngestStory`` can be constructed with this in place of a real
    ``PastedStorySource`` is the proof the injection seam the ``StorySource``
    Protocol exists for actually works - not merely that the Protocol is
    declared somewhere and nothing depends on it.
    """

    capabilities = CapabilityDescriptor(
        provider_id="fake",
        version="0",
        cost_model=CostModel.FREE,
        latency_class=LatencyClass.INSTANT,
        offline=True,
        requires_gpu=False,
        vram_mb=None,
        quality_tier=5,
        languages=frozenset({"und"}),
    )

    def __init__(self, text: str) -> None:
        self._text = text
        self.requested: list[str] = []

    def fetch(self, reference: str) -> str:
        self.requested.append(reference)
        return self._text


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
    stage = IngestStory(cas=cas, source=PastedStorySource())
    assert stage.id == "ingest_story"
    assert stage.version == 1
    assert stage.depends_on == ()
    assert stage.settings_keys == ("story_digest",)


def test_the_stage_fingerprint_follows_the_story_digest_not_the_path(cas: CasStore) -> None:
    stage = IngestStory(cas=cas, source=PastedStorySource())
    fp_a = stage.fingerprint(_ctx(settings={"story_digest": "a" * 64, "story_path": "/x"}))
    fp_b = stage.fingerprint(_ctx(settings={"story_digest": "a" * 64, "story_path": "/y"}))
    fp_c = stage.fingerprint(_ctx(settings={"story_digest": "b" * 64, "story_path": "/x"}))
    assert fp_a == fp_b, "the path must not reach the fingerprint"
    assert fp_a != fp_c, "the digest must reach the fingerprint"


def test_the_fingerprint_provider_identity_comes_from_the_injected_source(cas: CasStore) -> None:
    """``fingerprint`` reads ``provider_id``/``provider_version`` off
    ``self._source.capabilities`` rather than hardcoding them - two stages
    differing only in which source they were given must disagree, or a
    provider swap would silently share edge-tts's cache entries with
    Piper's."""
    ctx = _ctx(settings={"story_digest": "a" * 64})
    pasted_stage = IngestStory(cas=cas, source=PastedStorySource())
    fake_stage = IngestStory(cas=cas, source=_FakeStorySource("irrelevant"))

    assert pasted_stage.fingerprint(ctx) != fake_stage.fingerprint(ctx)


def test_run_emits_story_txt_with_the_fetched_bytes(tmp_path: Path, cas: CasStore) -> None:
    story_path = tmp_path / "story.txt"
    story_path.write_text("Once upon a time.\n", encoding="utf-8")
    stage = IngestStory(cas=cas, source=PastedStorySource())
    ctx = _ctx(settings={"story_path": str(story_path), "story_digest": "a" * 64})

    result = stage.run(ctx, lambda fraction, note: None)

    artifact = result.artifact("story.txt")
    assert artifact.kind == "text"
    assert cas.read_bytes(artifact.digest) == b"Once upon a time.\n"


def test_run_propagates_a_fatal_provider_error_for_a_missing_story(
    tmp_path: Path, cas: CasStore
) -> None:
    """``run`` does not swallow the source's error - it is ``run_stage``
    (the worker's caller), not the stage, that translates it into a
    worker-protocol message."""
    stage = IngestStory(cas=cas, source=PastedStorySource())
    ctx = _ctx(settings={"story_path": str(tmp_path / "absent.txt"), "story_digest": "a" * 64})

    with pytest.raises(ProviderError) as exc:
        stage.run(ctx, lambda fraction, note: None)
    assert exc.value.kind is ErrorKind.FATAL


def test_run_reads_through_the_injected_storysource_not_a_hardcoded_one(
    tmp_path: Path, cas: CasStore
) -> None:
    """The whole point of typing ``IngestStory`` against ``StorySource``
    rather than ``PastedStorySource``: a fake must be substitutable, and the
    stage must never construct its own concrete source. A real file sits at
    ``story_path`` with different content than the fake's, so if
    ``IngestStory`` ever went back to hardcoding a concrete source, this
    test would catch it by staging the *real file's* content instead of the
    fake's."""
    real_file = tmp_path / "story.txt"
    real_file.write_text("the real file on disk\n", encoding="utf-8")
    fake = _FakeStorySource("a story only the fake could produce\n")
    stage = IngestStory(cas=cas, source=fake)
    ctx = _ctx(settings={"story_path": str(real_file), "story_digest": "a" * 64})

    result = stage.run(ctx, lambda fraction, note: None)

    assert fake.requested == [str(real_file)], "the fake must be the one asked to fetch"
    assert (
        cas.read_bytes(result.artifact("story.txt").digest)
        == b"a story only the fake could produce\n"
    )
