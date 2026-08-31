"""``PastedStorySource``, and ``make_stage``'s wiring of it into ``IngestStory``.

``IngestStory`` itself is typed against the ``StorySource`` Protocol and
lives in ``ytauto.app.stages.ingest_story`` - its behavioural tests are in
``tests/unit/app/stages/test_ingest_story.py``. What belongs here is
everything specific to this concrete provider: that ``fetch`` behaves
correctly, that its ``CapabilityDescriptor`` is honest, that it actually
satisfies the ``StorySource`` Protocol it claims to, and that ``make_stage``
- the one function allowed to import both sides of the ``app``/``providers``
boundary - wires the two together correctly.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ytauto.app.stages import ingest_story
from ytauto.app.stages.ingest_story import IngestStory
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.content_hash import hash_bytes
from ytauto.core.pipeline.stage import JobContext
from ytauto.core.ports.capability import CostModel
from ytauto.core.ports.providers import StorySource
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations
from ytauto.providers.story.pasted import PROVIDER_VERSION, PastedStorySource, make_stage


def test_the_capability_version_matches_the_stage_side_constant() -> None:
    """The provider's ``PROVIDER_VERSION`` docstring says to bump it when
    ``fetch``'s behaviour changes - but only ``ingest_story``'s own literal
    reaches ``stage_fingerprint``, so a bump here alone invalidates nothing
    and the next run serves the old ``story.txt`` from cache. Pinning the two
    equal turns that silent staleness into a failing gate: bump either
    constant and this test names the other.

    ``provider_id`` is pinned for the same reason one step further out - it
    is the other half of the identity pair the fingerprint hashes, and a
    provider renamed on one side only would silently share cache entries
    across two different providers."""
    assert PastedStorySource.capabilities.version == PROVIDER_VERSION
    assert PROVIDER_VERSION == ingest_story.PROVIDER_VERSION
    assert PastedStorySource.capabilities.provider_id == ingest_story.PROVIDER_ID


def test_a_pasted_story_is_read_verbatim(tmp_path: Path) -> None:
    path = tmp_path / "story.txt"
    path.write_text("The train never stopped.\n", encoding="utf-8")
    assert PastedStorySource().fetch(str(path)) == "The train never stopped.\n"


def test_a_missing_story_file_is_a_fatal_provider_error(tmp_path: Path) -> None:
    with pytest.raises(ProviderError) as exc:
        PastedStorySource().fetch(str(tmp_path / "absent.txt"))
    assert exc.value.kind is ErrorKind.FATAL, "a missing file will not appear on retry"


def test_a_story_file_that_is_not_valid_utf8_is_a_fatal_provider_error(tmp_path: Path) -> None:
    path = tmp_path / "story.txt"
    path.write_bytes(b"\xff\xfe not valid utf-8")
    with pytest.raises(ProviderError) as exc:
        PastedStorySource().fetch(str(path))
    assert exc.value.kind is ErrorKind.FATAL, "a bad encoding will not appear on retry"


def test_the_capability_descriptor_declares_a_free_offline_no_gpu_provider() -> None:
    """Ambiguity resolution #4: this reads a local file, so no GPU, no
    network, no cost - the descriptor must say so."""
    caps = PastedStorySource.capabilities
    assert caps.provider_id == "pasted"
    assert caps.cost_model is CostModel.FREE
    assert caps.offline is True
    assert caps.requires_gpu is False
    assert caps.vram_mb is None


def test_pasted_story_source_conforms_to_the_storysource_protocol() -> None:
    """``StorySource`` is ``@runtime_checkable`` specifically so this is
    cheap to check. Without it, ``PastedStorySource`` could silently drift
    from the Protocol ``IngestStory`` depends on - a renamed method or a
    dropped ``capabilities`` attribute would only surface as a confusing
    ``AttributeError`` deep inside a worker, since nothing else here
    statically confirms conformance (mypy does not check that an
    unannotated class satisfies a Protocol it never declares)."""
    assert isinstance(PastedStorySource(), StorySource)


def test_make_stage_wires_a_pastedstorysource_into_ingest_story(tmp_path: Path) -> None:
    """``make_stage`` is the one function allowed to import both
    ``app.stages.ingest_story`` and this module's own ``PastedStorySource``;
    this is the test that its wiring actually produces a working stage."""
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    try:
        cas = CasStore(root=tmp_path / "cas", conn=conn)
        story_path = tmp_path / "story.txt"
        story_path.write_text("hello\n", encoding="utf-8")

        stage = make_stage(cas=cas, settings={"voice": "en-GB-RyanNeural"})
        assert isinstance(stage, IngestStory)

        ctx = JobContext(
            job_id="j1",
            project_id="p1",
            settings={
                "story_path": str(story_path),
                # The real digest: IngestStory.run refuses to stage content
                # that does not hash to story_digest.
                "story_digest": str(hash_bytes(b"hello\n")),
            },
            inputs={},
            workdir=tmp_path,
        )
        result = stage.run(ctx, lambda fraction, note: None)

        assert cas.read_bytes(result.artifact("story.txt").digest) == b"hello\n"
    finally:
        conn.close()


def test_make_stage_ignores_settings_it_has_no_use_for(tmp_path: Path) -> None:
    """``make_stage`` accepts the project's whole settings per the uniform
    factory contract but makes no decision from them - this stage has
    exactly one provider. An unrelated or even nonsensical settings mapping
    must not prevent construction."""
    conn = sqlite3.connect(":memory:")
    try:
        cas = CasStore(root=tmp_path / "cas", conn=conn)
        stage = make_stage(cas=cas, settings={})
        assert stage.id == "ingest_story"
    finally:
        conn.close()
