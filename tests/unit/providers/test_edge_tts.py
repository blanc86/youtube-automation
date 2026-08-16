"""``EdgeTtsSynthesizer``, and ``make_stage``'s wiring of it into
``synthesize_speech``.

``SynthesizeSpeech`` itself is typed against the ``SpeechSynthesizer``
Protocol and lives in ``ytauto.app.stages.synthesize_speech`` - its
behavioural tests are in ``tests/unit/app/stages/test_synthesize_speech.py``.
What belongs here is everything specific to this concrete provider: that
``synthesize``'s accumulation and error-mapping logic is correct, that its
``CapabilityDescriptor`` is honest, that it conforms to the
``SpeechSynthesizer`` Protocol it claims to, and that ``make_stage`` wires
the two together correctly.

This is the first network-touching provider in the project. Every test here
goes through ``_consume`` - the exact seam ``EdgeTtsSynthesizer.synthesize``
itself uses - with a fake async stream standing in for a real
``edge_tts.Communicate(...).stream()``, or patches ``edge_tts.Communicate``
outright. No test in this module opens a websocket.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import edge_tts
import pytest

from ytauto.app.stages.synthesize_speech import SynthesizeSpeech
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.narration import Narration, WordBoundary
from ytauto.core.pipeline.stage import JobContext
from ytauto.core.ports.capability import CostModel
from ytauto.core.ports.providers import SpeechSynthesizer
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations
from ytauto.providers.tts.edge import EdgeTtsSynthesizer, _consume, make_stage

# ---------------------------------------------------------------------------
# The fake-stream seam. `_consume` is the same function `synthesize` calls;
# feeding it a fake async generator exercises the real accumulation and
# error-mapping logic with no network involved.
# ---------------------------------------------------------------------------


async def _fake_stream(events: list[dict[str, Any]]) -> AsyncIterator[dict[str, Any]]:
    for event in events:
        yield event


async def _raising_stream(exc: Exception) -> AsyncIterator[dict[str, Any]]:
    raise exc
    yield  # pragma: no cover - unreachable; makes this an async generator


def _synthesize_from(events: list[dict[str, Any]]) -> Narration:
    return _consume(_fake_stream(events))


def _synthesize_raising(exc: Exception) -> Narration:
    return _consume(_raising_stream(exc))


def test_word_boundary_events_become_word_boundaries() -> None:
    """edge-tts reports offsets in 100-nanosecond ticks; we store seconds."""
    events = [
        {"type": "WordBoundary", "offset": 1_000_000, "duration": 5_000_000, "text": "Hello"},
        {"type": "audio", "data": b"\x00"},
    ]
    narration = _synthesize_from(events)
    assert narration.boundaries == (WordBoundary(text="Hello", start_s=0.1, duration_s=0.5),)


def test_audio_chunks_are_concatenated_in_order() -> None:
    events = [
        {"type": "audio", "data": b"aa"},
        {"type": "audio", "data": b"bb"},
    ]
    assert _synthesize_from(events).audio == b"aabb"


def test_a_network_failure_is_retryable() -> None:
    with pytest.raises(ProviderError) as exc:
        _synthesize_raising(ConnectionError("no route to host"))
    assert exc.value.kind is ErrorKind.RETRYABLE


def test_an_unknown_voice_is_fatal() -> None:
    with pytest.raises(ProviderError) as exc:
        _synthesize_raising(ValueError("No audio was received. Please verify parameters"))
    assert exc.value.kind is ErrorKind.FATAL, "a typo in a voice name will not fix itself"


def test_a_well_formed_but_nonexistent_voice_is_also_fatal() -> None:
    """The brief's own ambiguity resolution #5 names ``ValueError`` as the
    exception edge-tts raises for a bad voice. Verified live against the
    installed package (7.2.8) during this task's implementation: a
    *malformed* voice string (``"not-a-real-voice"``) does raise
    ``ValueError``, synchronously, at ``Communicate()`` construction - but a
    *well-formed* voice name that simply does not exist
    (``"en-US-NonexistentNeural"``) raises ``edge_tts.exceptions.NoAudioReceived``
    instead, from inside the stream, which is not a ``ValueError`` subclass.
    Both are the same "a typo will not fix itself" case and must both be
    FATAL - this test is the one the brief's own text does not cover."""
    with pytest.raises(ProviderError) as exc:
        _synthesize_raising(
            edge_tts.exceptions.NoAudioReceived(
                "No audio was received. Please verify that your parameters are correct."
            )
        )
    assert exc.value.kind is ErrorKind.FATAL


def test_the_capability_descriptor_declares_a_free_networked_no_gpu_provider() -> None:
    caps = EdgeTtsSynthesizer.capabilities
    assert caps.provider_id == "edge-tts"
    assert caps.cost_model is CostModel.FREE
    assert caps.offline is False, "this provider calls Microsoft's service over the network"
    assert caps.requires_gpu is False
    assert caps.vram_mb is None


def test_edge_tts_synthesizer_conforms_to_the_speechsynthesizer_protocol() -> None:
    assert isinstance(EdgeTtsSynthesizer(), SpeechSynthesizer)


def test_synthesize_requests_word_boundaries_not_sentence_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``edge_tts.Communicate`` defaults ``boundary`` to ``"SentenceBoundary"``
    - confirmed live against the installed package during this task's
    implementation, and not mentioned by the brief's own Step 3 snippet, which
    constructs ``Communicate(text, voice, rate=rate)`` with no ``boundary``
    kwarg at all. Following that literally would silently downgrade every
    caption in the project to sentence-level, and this stage would never
    produce a usable ``boundaries.json``. Pinned here against a patched
    ``edge_tts.Communicate`` so a regression fails loudly without touching
    the network."""
    captured: dict[str, Any] = {}

    class _FakeCommunicate:
        def __init__(self, text: str, voice: str, **kwargs: Any) -> None:
            captured["text"] = text
            captured["voice"] = voice
            captured.update(kwargs)

        async def stream(self) -> AsyncIterator[dict[str, Any]]:
            yield {"type": "audio", "data": b"x"}

    monkeypatch.setattr(edge_tts, "Communicate", _FakeCommunicate)

    EdgeTtsSynthesizer(rate="+10%").synthesize("hello", voice="en-US-JennyNeural")

    assert captured["boundary"] == "WordBoundary"
    assert captured["rate"] == "+10%"
    assert captured["voice"] == "en-US-JennyNeural"


def test_make_stage_wires_an_edgettssynthesizer_into_synthesize_speech(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Mirrors ``test_pasted_story.py``'s wiring test: ``make_stage`` is the
    one function allowed to import both ``app.stages.synthesize_speech`` and
    this module's own ``EdgeTtsSynthesizer``; this proves the wiring actually
    produces a working stage end to end - through a patched
    ``edge_tts.Communicate`` so it stays hermetic."""
    captured: dict[str, Any] = {}

    class _FakeCommunicate:
        def __init__(self, text: str, voice: str, **kwargs: Any) -> None:
            captured["text"] = text
            captured.update(kwargs)

        async def stream(self) -> AsyncIterator[dict[str, Any]]:
            yield {"type": "WordBoundary", "offset": 0, "duration": 10_000_000, "text": "hi"}
            yield {"type": "audio", "data": b"mp3-bytes"}

    monkeypatch.setattr(edge_tts, "Communicate", _FakeCommunicate)

    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    try:
        cas = CasStore(root=tmp_path / "cas", conn=conn)
        story_digest = cas.stage_file(b"hi\n", kind="text")

        stage = make_stage(cas=cas, settings={"voice": "en-US-JennyNeural", "rate": "+5%"})
        assert isinstance(stage, SynthesizeSpeech)

        ctx = JobContext(
            job_id="j1",
            project_id="p1",
            settings={"voice": "en-US-JennyNeural", "rate": "+5%"},
            inputs={
                "ingest_story": (ArtifactRef(name="story.txt", kind="text", digest=story_digest),)
            },
            workdir=tmp_path,
        )
        result = stage.run(ctx, lambda fraction, note: None)

        assert cas.read_bytes(result.artifact("narration.mp3").digest) == b"mp3-bytes"
        boundaries = json.loads(cas.read_bytes(result.artifact("boundaries.json").digest))
        assert boundaries == [{"text": "hi", "start_s": 0.0, "duration_s": 1.0}]
        assert captured["rate"] == "+5%"
        assert captured["text"] == "hi\n"
    finally:
        conn.close()


def test_make_stage_defaults_rate_when_the_project_has_not_set_one(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: dict[str, Any] = {}

    class _FakeCommunicate:
        def __init__(self, text: str, voice: str, **kwargs: Any) -> None:
            captured.update(kwargs)

        async def stream(self) -> AsyncIterator[dict[str, Any]]:
            yield {"type": "audio", "data": b"x"}

    monkeypatch.setattr(edge_tts, "Communicate", _FakeCommunicate)
    conn = sqlite3.connect(":memory:")
    try:
        cas = CasStore(root=tmp_path / "cas", conn=conn)
        stage = make_stage(cas=cas, settings={})
        assert stage.id == "synthesize_speech"
        stage._synth.synthesize("hi", voice="en-US-JennyNeural")
        assert captured["rate"] == "+0%"
    finally:
        conn.close()
