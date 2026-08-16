"""``Transcribe``: the third stage of the ``story_video`` pipeline.

Typed against ``core.ports.providers.Transcriber``, never against a concrete
provider class - ``ytauto.app`` may not import ``ytauto.providers`` (an
import-linter ``forbidden`` contract). Every test here exercises ``Transcribe``
with a fake ``Transcriber`` injected, both because that keeps this suite off
the real ``EdgeBoundaryTranscriber`` (whose own behaviour is
``tests/unit/providers/test_edge_boundary.py``'s job to cover) and because
this file is the one this task's brief asks for explicitly: "if your stage
delegates to a provider, make sure at least one test drives the stage's own
``run()`` rather than only the provider in isolation." The brief's own Files
list names only ``tests/unit/providers/test_edge_boundary.py`` - that covers
``EdgeBoundaryTranscriber.transcribe`` but nothing about stage identity,
``settings_keys``, fingerprint provider-identity literalness, or that
``run()`` reads through the injected ``Transcriber`` rather than a
hardcoded one. This file exists to close that gap; see this task's report for
why it was added even though the brief's Step list did not ask for it.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path

import pytest

from ytauto.app.stages.transcribe import Transcribe
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash
from ytauto.core.models.narration import Narration, WordBoundary
from ytauto.core.pipeline.stage import JobContext
from ytauto.core.ports.capability import CapabilityDescriptor, CostModel, LatencyClass
from ytauto.infra.cas.store import CasStore

# db_conn is defined in tests/unit/conftest.py.

_TranscribeResult = tuple[tuple[str, float, float], ...]


class _FakeTranscriber:
    """A ``Transcriber`` double that never touches ``EdgeBoundaryTranscriber``.

    Its ``capabilities`` deliberately differ from ``EdgeBoundaryTranscriber``'s
    so that a stage which (wrongly) read provider identity off the injected
    object would fingerprint differently once this is substituted in - the
    exact failure mode
    ``test_the_fingerprint_provider_identity_is_literal_not_injected`` guards
    against.
    """

    capabilities = CapabilityDescriptor(
        provider_id="fake-transcriber",
        version="99",
        cost_model=CostModel.FREE,
        latency_class=LatencyClass.INSTANT,
        offline=True,
        requires_gpu=False,
        vram_mb=None,
        quality_tier=1,
        languages=frozenset({"und"}),
    )

    def __init__(self, result: _TranscribeResult) -> None:
        self._result = result
        self.calls: list[Narration] = []

    def transcribe(self, narration: Narration) -> _TranscribeResult:
        self.calls.append(narration)
        return self._result


class _RaisingTranscriber:
    """A ``Transcriber`` double whose ``transcribe`` always fails."""

    capabilities = _FakeTranscriber.capabilities

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def transcribe(self, narration: Narration) -> _TranscribeResult:
        raise self._exc


class _OtherFakeTranscriber:
    """A second, distinct ``Transcriber`` double.

    A genuinely different class from ``_FakeTranscriber`` - not just a second
    instance of it - with its own ``provider_id``/``version`` on
    ``capabilities``. Mirrors ``test_synthesize_speech.py``'s
    ``_OtherFakeSynthesizer``: two *instances* of ``_FakeTranscriber`` share
    one class-level ``capabilities`` object, so swapping one instance for
    another would prove nothing about whether ``fingerprint`` reads identity
    off the injected object or off a literal.
    """

    capabilities = CapabilityDescriptor(
        provider_id="other-fake-transcriber",
        version="1",
        cost_model=CostModel.FREE,
        latency_class=LatencyClass.INSTANT,
        offline=True,
        requires_gpu=False,
        vram_mb=None,
        quality_tier=1,
        languages=frozenset({"und"}),
    )

    def __init__(self, result: _TranscribeResult) -> None:
        self._result = result

    def transcribe(self, narration: Narration) -> _TranscribeResult:
        return self._result


_ARBITRARY_DIGEST = ContentHash("a" * 64)
_OTHER_DIGEST = ContentHash("b" * 64)


@pytest.fixture()
def cas(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    return CasStore(root=tmp_path / "cas", conn=db_conn)


def _ctx(
    *,
    settings: Mapping[str, object] | None = None,
    boundaries_digest: ContentHash = _ARBITRARY_DIGEST,
    workdir: Path = Path("/tmp/j1"),
) -> JobContext:
    return JobContext(
        job_id="j1",
        project_id="p1",
        settings={} if settings is None else settings,
        inputs={
            "synthesize_speech": (
                ArtifactRef(name="boundaries.json", kind="json", digest=boundaries_digest),
            )
        },
        workdir=workdir,
    )


def test_stage_identity_and_declared_settings(cas: CasStore) -> None:
    stage = Transcribe(cas=cas, transcriber=_FakeTranscriber(()))
    assert stage.id == "transcribe"
    assert stage.version == 1
    assert stage.depends_on == ("synthesize_speech",)
    assert stage.settings_keys == (), "this stage reads no project settings"


def test_the_fingerprint_provider_identity_is_literal_not_injected(cas: CasStore) -> None:
    """This task's brief: "Use literal constants for provider identity, as
    Task 5 does - not values read off the injected provider's capabilities."
    Two stages differing only in which ``Transcriber`` they were given must
    fingerprint identically, or a factory that later picks a provider from
    settings (boundary-replay vs. a future ASR-based one) would make the
    dispatcher and a worker compute different fingerprints for what the
    dispatcher still thinks is one cached stage."""
    ctx = _ctx()
    stage_a = Transcribe(cas=cas, transcriber=_FakeTranscriber(()))
    stage_b = Transcribe(cas=cas, transcriber=_OtherFakeTranscriber(()))
    assert stage_a.fingerprint(ctx) == stage_b.fingerprint(ctx)


def test_the_fingerprint_follows_the_upstream_boundaries_digest(cas: CasStore) -> None:
    """``settings_keys`` is ``()``, so nothing in ``ctx.settings`` can
    invalidate a cached fingerprint - the *only* thing that may is
    ``ctx.inputs``, which ``stage_fingerprint`` also hashes. Without this, a
    re-synthesised narration (new ``boundaries.json``, e.g. after a voice
    change upstream) would be served this stage's stale cached output."""
    stage = Transcribe(cas=cas, transcriber=_FakeTranscriber(()))
    fp_a = stage.fingerprint(_ctx(boundaries_digest=_ARBITRARY_DIGEST))
    fp_b = stage.fingerprint(_ctx(boundaries_digest=_OTHER_DIGEST))
    assert fp_a != fp_b, "a new upstream boundaries.json must invalidate the cache"


def test_run_emits_word_timings_json(cas: CasStore) -> None:
    boundaries_bytes = json.dumps(
        [
            {"text": "Hello", "start_s": 0.1, "duration_s": 0.5},
            {"text": "world", "start_s": 0.7, "duration_s": 0.3},
        ]
    ).encode("utf-8")
    boundaries_digest = cas.stage_file(boundaries_bytes, kind="json")
    fake = _FakeTranscriber((("Hello", 0.1, 0.6), ("world", 0.7, 1.0)))
    stage = Transcribe(cas=cas, transcriber=fake)
    ctx = _ctx(boundaries_digest=boundaries_digest)

    result = stage.run(ctx, lambda fraction, note: None)

    timings_ref = result.artifact("word_timings.json")
    assert timings_ref.kind == "json"
    assert json.loads(cas.read_bytes(timings_ref.digest)) == [
        ["Hello", 0.1, 0.6],
        ["world", 0.7, 1.0],
    ]


def test_run_reconstructs_wordboundary_objects_from_the_json(cas: CasStore) -> None:
    """``boundaries.json`` was written by ``synthesize_speech`` as
    ``json.dumps([asdict(b) for b in boundaries])`` - this proves the decode
    is the exact inverse: the injected ``Transcriber`` must see a real
    ``Narration`` whose ``boundaries`` are ``WordBoundary`` objects equal to
    the ones that were originally serialised, and whose ``audio`` is
    ``b""``."""
    boundaries_bytes = json.dumps([{"text": "Hello", "start_s": 0.1, "duration_s": 0.5}]).encode(
        "utf-8"
    )
    boundaries_digest = cas.stage_file(boundaries_bytes, kind="json")
    fake = _FakeTranscriber((("Hello", 0.1, 0.6),))
    stage = Transcribe(cas=cas, transcriber=fake)
    ctx = _ctx(boundaries_digest=boundaries_digest)

    stage.run(ctx, lambda fraction, note: None)

    assert len(fake.calls) == 1
    narration = fake.calls[0]
    assert narration.audio == b""
    assert narration.boundaries == (WordBoundary(text="Hello", start_s=0.1, duration_s=0.5),)


def test_run_handles_an_empty_boundaries_array(cas: CasStore) -> None:
    boundaries_digest = cas.stage_file(b"[]", kind="json")
    fake = _FakeTranscriber(())
    stage = Transcribe(cas=cas, transcriber=fake)
    ctx = _ctx(boundaries_digest=boundaries_digest)

    result = stage.run(ctx, lambda fraction, note: None)

    assert json.loads(cas.read_bytes(result.artifact("word_timings.json").digest)) == []
    assert fake.calls[0].boundaries == ()


def test_run_propagates_a_provider_error_from_the_injected_transcriber(cas: CasStore) -> None:
    """``run`` does not swallow the transcriber's error - it is ``run_stage``
    (the worker's caller), not the stage, that translates it into a
    worker-protocol message."""
    boundaries_digest = cas.stage_file(b"[]", kind="json")
    stage = Transcribe(
        cas=cas,
        transcriber=_RaisingTranscriber(
            ProviderError("boom", provider_id="fake-transcriber", kind=ErrorKind.RETRYABLE)
        ),
    )
    ctx = _ctx(boundaries_digest=boundaries_digest)

    with pytest.raises(ProviderError) as exc:
        stage.run(ctx, lambda fraction, note: None)
    assert exc.value.kind is ErrorKind.RETRYABLE


def test_run_reads_through_the_injected_transcriber_not_a_hardcoded_one(cas: CasStore) -> None:
    """The whole point of typing ``Transcribe`` against ``Transcriber`` rather
    than ``EdgeBoundaryTranscriber``: a fake must be substitutable, and the
    stage must never construct its own concrete transcriber."""
    boundaries_digest = cas.stage_file(b"[]", kind="json")
    fake = _FakeTranscriber((("only-the-fake-could-produce-this", 0.0, 1.0),))
    stage = Transcribe(cas=cas, transcriber=fake)
    ctx = _ctx(boundaries_digest=boundaries_digest)

    result = stage.run(ctx, lambda fraction, note: None)

    assert json.loads(cas.read_bytes(result.artifact("word_timings.json").digest)) == [
        ["only-the-fake-could-produce-this", 0.0, 1.0]
    ]
