"""``EdgeBoundaryTranscriber``, and ``make_stage``'s wiring of it into
``Transcribe``.

``Transcribe`` itself is typed against the ``Transcriber`` Protocol and lives
in ``ytauto.app.stages.transcribe`` - its behavioural tests are in
``tests/unit/app/stages/test_transcribe.py``. What belongs here is everything
specific to this concrete provider: that ``transcribe`` behaves correctly
(including the FATAL "you switched to Piper, you now need Whisper" seam),
that it never reads ``narration.audio``, that its ``CapabilityDescriptor`` is
honest, that it actually satisfies the ``Transcriber`` Protocol it claims to,
and that ``make_stage`` - the one function allowed to import both sides of the
``app``/``providers`` boundary - wires the two together correctly.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import cast

import pytest

from ytauto.app.stages import transcribe as transcribe_stage
from ytauto.app.stages.transcribe import Transcribe
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.narration import Narration, WordBoundary
from ytauto.core.pipeline.stage import JobContext
from ytauto.core.ports.capability import CostModel
from ytauto.core.ports.providers import Transcriber
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations
from ytauto.providers.transcribe.edge_boundary import (
    PROVIDER_VERSION,
    EdgeBoundaryTranscriber,
    make_stage,
)


class _PoisonAudio:
    """Raises the instant anything touches it.

    Passed as ``Narration.audio`` (via ``cast(bytes, ...)`` so the type
    checker treats it as ``bytes``) to prove
    ``EdgeBoundaryTranscriber.transcribe`` never reads audio at all when
    boundaries are present - ambiguity resolution #5 in this task's brief:
    "``EdgeBoundaryTranscriber`` must not read ``audio`` at all, and that is
    worth a test." A merely-wrong-looking bytes value (``b"garbage"``) would
    only prove the *content* of ``audio`` does not affect the result; this
    object proves the attribute is never even inspected, since any read at
    all - ``len()``, iteration, an attribute access - fails the test
    immediately rather than silently producing a plausible-looking answer.
    """

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"EdgeBoundaryTranscriber touched narration.audio.{name}")

    def __len__(self) -> int:
        raise AssertionError("EdgeBoundaryTranscriber called len(narration.audio)")

    def __iter__(self) -> object:
        raise AssertionError("EdgeBoundaryTranscriber iterated narration.audio")

    def __bool__(self) -> bool:
        raise AssertionError("EdgeBoundaryTranscriber checked truthiness of narration.audio")


def test_boundaries_become_start_end_triples() -> None:
    """Verbatim from this task's brief, Step 1."""
    narration = Narration(
        audio=b"",
        boundaries=(
            WordBoundary(text="Hello", start_s=0.1, duration_s=0.5),
            WordBoundary(text="world", start_s=0.7, duration_s=0.3),
        ),
    )
    assert EdgeBoundaryTranscriber().transcribe(narration) == (
        ("Hello", 0.1, 0.6),
        ("world", 0.7, 1.0),
    )


def test_absent_boundaries_are_fatal_and_say_why() -> None:
    """Verbatim from this task's brief, Step 1: "the 'you switched to Piper,
    you now need Whisper' seam." An audio-only ``SpeechSynthesizer`` sets
    ``Narration.boundaries`` to ``None``; a boundary-consuming transcriber
    must refuse loudly rather than fabricate timings that would drift
    captions out of sync in a way far harder to notice than a failed stage."""
    with pytest.raises(ProviderError) as exc:
        EdgeBoundaryTranscriber().transcribe(Narration(audio=b"x", boundaries=None))
    assert exc.value.kind is ErrorKind.FATAL
    assert "boundaries" in str(exc.value)


def test_transcribe_never_reads_audio_bytes() -> None:
    """Ambiguity resolution #5: the whole reason this path is free is that
    the timings already exist as ``WordBoundary`` objects - ``audio`` must
    never be touched, on the boundaries-present path, at all."""
    narration = Narration(
        audio=cast(bytes, _PoisonAudio()),
        boundaries=(WordBoundary(text="hi", start_s=0.0, duration_s=1.0),),
    )
    assert EdgeBoundaryTranscriber().transcribe(narration) == (("hi", 0.0, 1.0),)


def test_end_s_matches_the_wordboundary_property_not_a_recomputation() -> None:
    """Ambiguity resolution #2: ``end_s`` must come from ``WordBoundary.end_s``
    - a boundary whose stored fields would give a different answer if
    ``start_s + duration_s`` were computed a second, independent way (it
    should not be possible to construct one, since ``end_s`` is a property
    derived from those exact two fields) is not the point here; the point is
    that this provider has only one definition of "when does a word stop"."""
    boundary = WordBoundary(text="hi", start_s=1.25, duration_s=0.75)
    narration = Narration(audio=b"", boundaries=(boundary,))
    ((_, _, end_s),) = EdgeBoundaryTranscriber().transcribe(narration)
    assert end_s == boundary.end_s == 2.0


def test_the_capability_descriptor_declares_a_free_offline_no_gpu_provider() -> None:
    caps = EdgeBoundaryTranscriber.capabilities
    assert caps.provider_id == "edge-boundary"
    assert caps.cost_model is CostModel.FREE
    assert caps.offline is True, "replaying already-captured boundaries touches no network"
    assert caps.requires_gpu is False
    assert caps.vram_mb is None


def test_edge_boundary_transcriber_conforms_to_the_transcriber_protocol() -> None:
    """``Transcriber`` is ``@runtime_checkable`` specifically so this is cheap
    to check - without it, ``EdgeBoundaryTranscriber`` could silently drift
    from the Protocol ``Transcribe`` depends on."""
    assert isinstance(EdgeBoundaryTranscriber(), Transcriber)


def test_make_stage_wires_an_edgeboundarytranscriber_into_transcribe(tmp_path: Path) -> None:
    """``make_stage`` is the one function allowed to import both
    ``app.stages.transcribe`` and this module's own
    ``EdgeBoundaryTranscriber``; this is the test that its wiring actually
    produces a working stage end to end."""
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    try:
        cas = CasStore(root=tmp_path / "cas", conn=conn)
        boundaries_bytes = json.dumps([{"text": "hi", "start_s": 0.0, "duration_s": 1.0}]).encode(
            "utf-8"
        )
        boundaries_digest = cas.stage_file(boundaries_bytes, kind="json")

        stage = make_stage(cas=cas, settings={})
        assert isinstance(stage, Transcribe)

        ctx = JobContext(
            job_id="j1",
            project_id="p1",
            settings={},
            inputs={
                "synthesize_speech": (
                    ArtifactRef(name="boundaries.json", kind="json", digest=boundaries_digest),
                )
            },
            workdir=tmp_path,
        )
        result = stage.run(ctx, lambda fraction, note: None)

        timings = json.loads(cas.read_bytes(result.artifact("word_timings.json").digest))
        assert timings == [["hi", 0.0, 1.0]]
    finally:
        conn.close()


def test_make_stage_ignores_settings_it_has_no_use_for(tmp_path: Path) -> None:
    """``make_stage`` accepts the project's whole settings per the uniform
    factory contract but makes no decision from them - this stage has exactly
    one provider and reads no settings at all (``settings_keys = ()``)."""
    conn = sqlite3.connect(":memory:")
    try:
        cas = CasStore(root=tmp_path / "cas", conn=conn)
        stage = make_stage(cas=cas, settings={"voice": "en-GB-RyanNeural", "unrelated": 123})
        assert stage.id == "transcribe"
    finally:
        conn.close()


def test_the_capability_version_matches_the_stage_side_constant() -> None:
    """This provider's ``PROVIDER_VERSION`` docstring says to bump it when
    ``transcribe``'s behaviour changes - but only the stage's own literal
    reaches ``stage_fingerprint``, so a bump here alone invalidates nothing
    and the next run serves stale artifacts from cache. Pinning the two equal
    turns that silent staleness into a failing gate: bump either constant and
    this test names the other.

    ``provider_id`` is pinned for the same reason one step further out - it is
    the other half of the identity pair the fingerprint hashes, and a provider
    renamed on one side only would silently share cache entries across two
    different providers."""
    assert EdgeBoundaryTranscriber.capabilities.version == PROVIDER_VERSION
    assert PROVIDER_VERSION == transcribe_stage.PROVIDER_VERSION
    assert EdgeBoundaryTranscriber.capabilities.provider_id == transcribe_stage.PROVIDER_ID
