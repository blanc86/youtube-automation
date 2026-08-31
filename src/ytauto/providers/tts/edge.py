"""``EdgeTtsSynthesizer``, and the entry-point factory that wires it into
``synthesize_speech``.

Mirrors ``providers/story/pasted.py``'s split: ``EdgeTtsSynthesizer`` (the
``SpeechSynthesizer`` port implementation) lives here under ``providers/``;
the stage that consumes it, ``SynthesizeSpeech``, lives in
``ytauto.app.stages.synthesize_speech`` typed against the ``SpeechSynthesizer``
Protocol, never against this concrete class, because ``ytauto.app`` may not
import ``ytauto.providers`` (an import-linter ``forbidden`` contract).
``make_stage`` below is the one thing allowed to import both sides of that
boundary, resolved by ``app/registry.py`` dynamically through
``importlib.metadata`` entry points rather than a static import - invisible
to import-linter, and exactly what keeps this the *only* place the seam is
crossed.

**Unlike ``SynthesizeSpeech.fingerprint``, this module's own
``PROVIDER_ID``/``PROVIDER_VERSION`` constants are never read by the stage.**
The stage carries its own literal copies (see its module docstring, and this
task's brief section "Do not copy Task 4's provider-identity pattern here").
The constants stay here anyway, on ``EdgeTtsSynthesizer.capabilities``,
because that descriptor is still honest metadata other callers (a future
cost policy, a provider picker) may legitimately consult - it is only the
*fingerprint* that must not.

**``boundary="WordBoundary"`` is not optional.** ``edge_tts.Communicate``
defaults its own ``boundary`` parameter to ``"SentenceBoundary"`` - confirmed
against the installed package (``Communicate.__init__``'s signature, and by a
live probe during this task's implementation: the same call with no
``boundary`` kwarg reports only ``SentenceBoundary`` events, never
``WordBoundary``). This project's entire free-captioning design rests on
per-word timing, so the constructor call below passes it explicitly;
``test_synthesize_requests_word_boundaries_not_sentence_boundaries`` pins
this against a patched ``edge_tts.Communicate`` so a regression fails loudly
without touching the network.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping

import edge_tts

from ytauto.app.stages.synthesize_speech import SynthesizeSpeech
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.narration import Narration, WordBoundary
from ytauto.core.ports.capability import CapabilityDescriptor, CostModel, LatencyClass
from ytauto.infra.cas.store import CasStore

PROVIDER_ID = "edge-tts"
PROVIDER_VERSION = "1"
"""Bump when ``EdgeTtsSynthesizer.synthesize``'s behaviour changes - and bump
``app.stages.synthesize_speech.PROVIDER_VERSION`` in the same commit.

This constant reaches ``capabilities`` and nothing else; only the stage-side
literal reaches ``stage_fingerprint`` (see the module docstring for why the
stage does not read this off ``capabilities``). Bumped alone it therefore
invalidates nothing, which is why
``test_the_capability_version_matches_the_stage_side_constant`` pins the two
equal - bumping either alone fails the gate."""

_TICKS_PER_SECOND = 10_000_000
"""edge-tts reports ``offset``/``duration`` in 100-nanosecond ticks; dividing
by this converts either to seconds."""

_DEFAULT_RATE = "+0%"
"""edge-tts's own default for ``Communicate(rate=...)`` - used by
``make_stage`` when a project has not set ``settings["rate"]``."""


async def _drain(stream: AsyncIterator[edge_tts.typing.TTSChunk]) -> Narration:
    """Accumulate one edge-tts event stream into a ``Narration``.

    Pure accumulation, no error handling and no network of its own - `stream``
    is just "something async-iterable that yields edge-tts-shaped chunks",
    which is what lets ``_consume`` (below) drive it from either the real
    ``Communicate(...).stream()`` or a fake test generator with no branching
    in this function at all.
    """
    audio = bytearray()
    boundaries: list[WordBoundary] = []
    async for chunk in stream:
        if chunk["type"] == "audio":
            audio += chunk["data"]
        elif chunk["type"] == "WordBoundary":
            boundaries.append(
                WordBoundary(
                    text=chunk["text"],
                    start_s=chunk["offset"] / _TICKS_PER_SECOND,
                    duration_s=chunk["duration"] / _TICKS_PER_SECOND,
                )
            )
    return Narration(audio=bytes(audio), boundaries=tuple(boundaries))


def _consume(make_stream: Callable[[], AsyncIterator[edge_tts.typing.TTSChunk]]) -> Narration:
    """Build a stream via ``make_stream`` and drain it, translating any
    exception - raised while *building* the stream or while *draining* it -
    into a ``ProviderError``.

    Takes a zero-argument **factory**, not an already-built stream, on
    purpose. Task 5's review found that an earlier version of this function
    took the stream directly, so ``EdgeTtsSynthesizer.synthesize`` had to
    build ``edge_tts.Communicate(...)`` *before* calling this function -
    outside this try/except entirely. That matters because
    ``Communicate.__init__`` validates ``voice``/``rate``/``volume``/``pitch``
    synchronously (``edge_tts.data_classes.TTSConfig.__post_init__``) and can
    raise a bare ``ValueError`` - for a malformed voice string like
    ``"not-a-real-voice"`` - before ``.stream()`` is ever called, let alone
    iterated. The old split left that specific call uncovered: the headline
    FATAL case the brief describes leaked past the mapping table entirely,
    caught only by ``run_stage``'s unrelated catch-all one layer up. Passing
    a factory here instead means ``EdgeTtsSynthesizer.synthesize`` can defer
    *all* of ``Communicate(...)`` - construction and iteration alike - to
    inside this one try/except, with nothing constructed outside it.

    This is the seam ``tests/unit/providers/test_edge_tts.py`` drives
    directly: ``_synthesize_from``/``_synthesize_raising`` wrap a fake stream
    in a zero-arg lambda the same shape ``EdgeTtsSynthesizer.synthesize``
    uses, and ``test_synthesize_maps_a_construction_time_error_to_fatal``
    exercises ``synthesize`` itself with a patched ``edge_tts.Communicate``
    that raises at ``__init__`` - the exact call path this factory shape
    exists to cover. No test opens a websocket.

    Raises:
        ProviderError: FATAL, for ``ValueError`` (edge-tts validates a
            malformed voice string, e.g. ``"not-a-real-voice"``, synchronously
            at ``Communicate()`` construction, before any network call) and
            for ``edge_tts.exceptions.NoAudioReceived`` (a well-formed voice
            name that simply does not exist - confirmed against the installed
            package: the server accepts the request but the stream ends with
            no audio). Neither will succeed on retry.
        ProviderError: RETRYABLE, for anything else - a dropped connection, a
            websocket error, a timeout. The service being flaky right now says
            nothing about whether it will be flaky on the next attempt.
    """
    try:
        return asyncio.run(_drain(make_stream()))
    except (ValueError, edge_tts.exceptions.NoAudioReceived) as exc:
        raise ProviderError(str(exc), provider_id=PROVIDER_ID, kind=ErrorKind.FATAL) from exc
    except Exception as exc:
        raise ProviderError(str(exc), provider_id=PROVIDER_ID, kind=ErrorKind.RETRYABLE) from exc


class EdgeTtsSynthesizer:
    """Speech synthesis through Microsoft Edge's read-aloud service.

    Free, requires network, reports per-word timings when asked for them -
    see the module docstring for why asking for them is not the default.
    """

    capabilities = CapabilityDescriptor(
        provider_id=PROVIDER_ID,
        version=PROVIDER_VERSION,
        cost_model=CostModel.FREE,
        latency_class=LatencyClass.FAST,
        offline=False,
        requires_gpu=False,
        vram_mb=None,
        # Neural voices, but not a dedicated commercial TTS product - a
        # respectable middle tier, not the top.
        quality_tier=4,
        # The language actually spoken is whatever `voice` names at call
        # time, not something this descriptor can pin down in advance - "und"
        # signals "depends on the caller's choice", the same reading
        # `providers/story/pasted.py` gives it for its own caller-controlled
        # seam.
        languages=frozenset({"und"}),
    )

    def __init__(self, *, rate: str = _DEFAULT_RATE) -> None:
        self._rate = rate

    def synthesize(self, text: str, *, voice: str) -> Narration:
        """Return the synthesised audio and its word boundaries.

        ``edge_tts.Communicate(...)`` is not constructed here directly - it is
        deferred into ``make_stream``, a zero-argument closure passed to
        ``_consume``, so that *both* its own synchronous validation (which
        can raise) and the network call its ``.stream()`` makes fall inside
        one shared error-mapping boundary. See ``_consume``'s docstring for
        why an earlier version of this split got that wrong.

        Raises:
            ProviderError: FATAL, for a malformed voice string (edge-tts
                validates ``voice``/``rate`` synchronously when
                ``Communicate(...)`` is constructed, before any network call)
                or a well-formed voice name that does not exist (discovered
                only after a real request - see ``_consume``). Neither will
                succeed on retry.
            ProviderError: RETRYABLE, for anything else raised while building
                or draining the stream - a dropped connection, a websocket
                error, a timeout.
        """

        def make_stream() -> AsyncIterator[edge_tts.typing.TTSChunk]:
            return edge_tts.Communicate(
                text, voice, rate=self._rate, boundary="WordBoundary"
            ).stream()

        return _consume(make_stream)


def make_stage(*, cas: CasStore, settings: Mapping[str, object]) -> SynthesizeSpeech:
    """Entry point ``story_video:synthesize_speech``.

    Constructs ``EdgeTtsSynthesizer`` unconditionally - no branch on
    ``settings`` picks between engines. Provider selection from settings
    (edge-tts vs. Piper vs. ElevenLabs) is a Phase 2b concern this plan
    deliberately defers; see ``synthesize_speech.py``'s module docstring for
    why doing that here, with a fingerprint that read identity off the
    injected object, would make the dispatcher and a worker disagree.

    ``rate`` is read from ``settings`` and baked into the synthesizer at
    construction rather than threaded through ``synthesize()`` (whose
    signature - fixed by the ``SpeechSynthesizer`` port - takes no ``rate``
    parameter at all). This is safe because it never reaches the fingerprint
    through this object: ``SynthesizeSpeech.fingerprint`` hashes
    ``ctx.settings["rate"]`` directly (via ``settings_keys``), and a worker
    always calls this factory with the very same settings mapping it builds
    that ``ctx`` from (``app/worker.py``'s ``main`` passes
    ``assignment["settings"]`` to both) - so whatever value ends up baked in
    here is, for any job that actually runs, the same value the fingerprint
    already accounted for.
    """
    rate = str(settings.get("rate", _DEFAULT_RATE))
    return SynthesizeSpeech(cas=cas, synthesizer=EdgeTtsSynthesizer(rate=rate))
