"""``SynthesizeSpeech``: the second stage of the ``story_video`` pipeline.

Typed against ``core.ports.providers.SpeechSynthesizer``, never against a
concrete provider class - ``ytauto.app`` may not import ``ytauto.providers``
(an import-linter ``forbidden`` contract). Most tests here exercise
``SynthesizeSpeech`` with a fake ``SpeechSynthesizer`` injected, both because
that keeps this suite off the network and because
``test_the_fingerprint_provider_identity_is_literal_not_injected`` below is
the one that matters structurally: it is the proof this stage does *not*
repeat ``IngestStory``'s pattern of reading provider identity off the
injected object's ``capabilities``. See this task's brief section "Do not
copy Task 4's provider-identity pattern here", and
``app/stages/synthesize_speech.py``'s own module docstring.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path

import pytest

from ytauto.app.stages.synthesize_speech import SynthesizeSpeech
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash
from ytauto.core.models.narration import Narration, WordBoundary
from ytauto.core.pipeline.stage import JobContext
from ytauto.core.ports.capability import CapabilityDescriptor, CostModel, LatencyClass
from ytauto.infra.cas.store import CasStore

# db_conn is defined in tests/unit/conftest.py.


class _FakeSynthesizer:
    """A ``SpeechSynthesizer`` double that never touches the network.

    Its ``capabilities`` deliberately differ from ``EdgeTtsSynthesizer``'s so
    that a stage which (wrongly) read provider identity off the injected
    object would fingerprint differently once this is substituted in - the
    exact failure mode
    ``test_the_fingerprint_provider_identity_is_literal_not_injected``
    guards against.
    """

    capabilities = CapabilityDescriptor(
        provider_id="fake-tts",
        version="99",
        cost_model=CostModel.FREE,
        latency_class=LatencyClass.INSTANT,
        offline=True,
        requires_gpu=False,
        vram_mb=None,
        quality_tier=1,
        languages=frozenset({"und"}),
    )

    def __init__(self, narration: Narration) -> None:
        self._narration = narration
        self.calls: list[tuple[str, str]] = []

    def synthesize(self, text: str, *, voice: str) -> Narration:
        self.calls.append((text, voice))
        return self._narration


class _RaisingSynthesizer:
    """A ``SpeechSynthesizer`` double whose ``synthesize`` always fails."""

    capabilities = _FakeSynthesizer.capabilities

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def synthesize(self, text: str, *, voice: str) -> Narration:
        raise self._exc


class _OtherFakeSynthesizer:
    """A second, distinct ``SpeechSynthesizer`` double.

    A genuinely different class from ``_FakeSynthesizer`` - not just a second
    instance of it - with its own ``provider_id``/``version`` on
    ``capabilities``. Two *instances* of ``_FakeSynthesizer`` share one
    class-level ``capabilities`` object, so swapping one instance for another
    in ``test_the_fingerprint_provider_identity_is_literal_not_injected``
    would prove nothing about whether ``fingerprint`` reads identity off the
    injected object or off a literal - both paths would compute the same
    hash either way. This class exists so that test's two stages differ in
    *provider identity itself*, which is the one axis that distinguishes the
    two implementations.
    """

    capabilities = CapabilityDescriptor(
        provider_id="other-fake-tts",
        version="1",
        cost_model=CostModel.FREE,
        latency_class=LatencyClass.INSTANT,
        offline=True,
        requires_gpu=False,
        vram_mb=None,
        quality_tier=1,
        languages=frozenset({"und"}),
    )

    def __init__(self, narration: Narration) -> None:
        self._narration = narration

    def synthesize(self, text: str, *, voice: str) -> Narration:
        return self._narration


_NARRATION = Narration(
    audio=b"mp3-bytes",
    boundaries=(WordBoundary(text="hi", start_s=0.0, duration_s=1.0),),
)
_ARBITRARY_DIGEST = ContentHash("a" * 64)


@pytest.fixture()
def cas(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    return CasStore(root=tmp_path / "cas", conn=db_conn)


def _ctx(
    *,
    settings: Mapping[str, object] | None = None,
    story_digest: ContentHash = _ARBITRARY_DIGEST,
    workdir: Path = Path("/tmp/j1"),
) -> JobContext:
    return JobContext(
        job_id="j1",
        project_id="p1",
        settings={"voice": "en-US-JennyNeural", "rate": "+0%"} if settings is None else settings,
        inputs={"ingest_story": (ArtifactRef(name="story.txt", kind="text", digest=story_digest),)},
        workdir=workdir,
    )


def test_stage_identity_and_declared_settings(cas: CasStore) -> None:
    stage = SynthesizeSpeech(cas=cas, synthesizer=_FakeSynthesizer(_NARRATION))
    assert stage.id == "synthesize_speech"
    assert stage.version == 1
    assert stage.depends_on == ("ingest_story",)
    assert stage.settings_keys == ("voice", "rate")


def test_the_fingerprint_follows_voice_and_rate(cas: CasStore) -> None:
    stage = SynthesizeSpeech(cas=cas, synthesizer=_FakeSynthesizer(_NARRATION))
    fp_a = stage.fingerprint(_ctx(settings={"voice": "en-US-JennyNeural", "rate": "+0%"}))
    fp_b = stage.fingerprint(_ctx(settings={"voice": "en-US-JennyNeural", "rate": "+10%"}))
    fp_c = stage.fingerprint(_ctx(settings={"voice": "en-GB-RyanNeural", "rate": "+0%"}))
    assert fp_a != fp_b, "rate must reach the fingerprint"
    assert fp_a != fp_c, "voice must reach the fingerprint"


def test_the_fingerprint_provider_identity_is_literal_not_injected(cas: CasStore) -> None:
    """Ambiguity resolution #4, and the brief's "Do not copy Task 4's
    provider-identity pattern here": two stages differing only in which
    ``SpeechSynthesizer`` they were given must fingerprint identically, or a
    factory that later picks a provider from settings would make the
    dispatcher and a worker compute different fingerprints for what the
    dispatcher still thinks is one cached stage. Contrast with
    ``test_ingest_story.py``'s own
    ``test_the_fingerprint_provider_identity_comes_from_the_injected_source``,
    which asserts the *opposite* for ``IngestStory`` - deliberately, since
    that stage's factory injects unconditionally and this task's whole point
    is that ``SynthesizeSpeech`` cannot yet rely on the same guarantee."""
    ctx = _ctx()
    stage_a = SynthesizeSpeech(cas=cas, synthesizer=_FakeSynthesizer(_NARRATION))
    stage_b = SynthesizeSpeech(cas=cas, synthesizer=_OtherFakeSynthesizer(_NARRATION))
    assert stage_a.fingerprint(ctx) == stage_b.fingerprint(ctx)


def test_run_emits_narration_mp3_and_boundaries_json(cas: CasStore) -> None:
    story_digest = cas.stage_file(b"Once upon a time.\n", kind="text")
    fake = _FakeSynthesizer(_NARRATION)
    stage = SynthesizeSpeech(cas=cas, synthesizer=fake)
    ctx = _ctx(story_digest=story_digest)

    result = stage.run(ctx, lambda fraction, note: None)

    audio = result.artifact("narration.mp3")
    assert audio.kind == "audio"
    assert cas.read_bytes(audio.digest) == b"mp3-bytes"

    boundaries_ref = result.artifact("boundaries.json")
    assert boundaries_ref.kind == "json"
    assert json.loads(cas.read_bytes(boundaries_ref.digest)) == [
        {"text": "hi", "start_s": 0.0, "duration_s": 1.0}
    ]
    assert fake.calls == [("Once upon a time.\n", "en-US-JennyNeural")]


def test_run_propagates_a_provider_error_from_the_injected_synthesizer(cas: CasStore) -> None:
    """``run`` does not swallow the synthesizer's error - it is ``run_stage``
    (the worker's caller), not the stage, that translates it into a
    worker-protocol message."""
    story_digest = cas.stage_file(b"text\n", kind="text")
    stage = SynthesizeSpeech(
        cas=cas,
        synthesizer=_RaisingSynthesizer(
            ProviderError("boom", provider_id="fake-tts", kind=ErrorKind.RETRYABLE)
        ),
    )
    ctx = _ctx(story_digest=story_digest)

    with pytest.raises(ProviderError) as exc:
        stage.run(ctx, lambda fraction, note: None)
    assert exc.value.kind is ErrorKind.RETRYABLE


def test_run_is_fatal_when_the_synthesizer_reports_no_boundaries(cas: CasStore) -> None:
    """``Narration.boundaries`` is ``None`` for audio-only engines (Piper,
    ElevenLabs, per that dataclass's own docstring) - a real possibility once
    a future task lets a provider be chosen from settings. This stage's whole
    output includes ``boundaries.json``, so pairing it with such a provider
    must fail loudly rather than write an empty or missing file."""
    story_digest = cas.stage_file(b"text\n", kind="text")
    stage = SynthesizeSpeech(
        cas=cas, synthesizer=_FakeSynthesizer(Narration(audio=b"x", boundaries=None))
    )
    ctx = _ctx(story_digest=story_digest)

    with pytest.raises(ProviderError) as exc:
        stage.run(ctx, lambda fraction, note: None)
    assert exc.value.kind is ErrorKind.FATAL, "missing boundaries is a config mistake, not a fluke"


def test_run_reads_through_the_injected_synthesizer_not_a_hardcoded_one(cas: CasStore) -> None:
    """The whole point of typing ``SynthesizeSpeech`` against
    ``SpeechSynthesizer`` rather than ``EdgeTtsSynthesizer``: a fake must be
    substitutable, and the stage must never construct its own concrete
    synthesizer."""
    story_digest = cas.stage_file(b"the real story\n", kind="text")
    fake = _FakeSynthesizer(Narration(audio=b"only the fake could produce this", boundaries=()))
    stage = SynthesizeSpeech(cas=cas, synthesizer=fake)
    ctx = _ctx(story_digest=story_digest)

    result = stage.run(ctx, lambda fraction, note: None)

    assert fake.calls == [("the real story\n", "en-US-JennyNeural")]
    assert (
        cas.read_bytes(result.artifact("narration.mp3").digest)
        == b"only the fake could produce this"
    )
