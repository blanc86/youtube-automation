"""``EdgeBoundaryTranscriber``, and the entry-point factory that wires it into
``transcribe``.

Mirrors ``providers/tts/edge.py``'s split: ``EdgeBoundaryTranscriber`` (the
``Transcriber`` port implementation) lives here under ``providers/``; the
stage that consumes it, ``Transcribe``, lives in
``ytauto.app.stages.transcribe`` typed against the ``Transcriber`` Protocol,
never against this concrete class, because ``ytauto.app`` may not import
``ytauto.providers`` (an import-linter ``forbidden`` contract). ``make_stage``
below is the one thing allowed to import both sides of that boundary,
resolved by ``app/registry.py`` dynamically through ``importlib.metadata``
entry points rather than a static import - invisible to import-linter.

This is the provider that makes the free path real. It runs no speech
recognition, opens no model, takes no GPU lease: it only replays the
word-boundary events ``EdgeTtsSynthesizer`` already captured while it
streamed (Task 5's ``boundaries.json``). Its entire cost is zero because the
work was already done for free by the TTS engine.

**Unlike ``Transcribe.fingerprint``, this module's own
``PROVIDER_ID``/``PROVIDER_VERSION`` constants are never read by the stage.**
The stage carries its own literal copies (see its module docstring). The
constants stay here anyway, on ``EdgeBoundaryTranscriber.capabilities``,
because that descriptor is still honest metadata other callers (a future cost
policy, a provider picker) may legitimately consult - it is only the
*fingerprint* that must not.
"""

from __future__ import annotations

from collections.abc import Mapping

from ytauto.app.stages.transcribe import Transcribe
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.narration import Narration
from ytauto.core.ports.capability import CapabilityDescriptor, CostModel, LatencyClass
from ytauto.infra.cas.store import CasStore

PROVIDER_ID = "edge-boundary"
PROVIDER_VERSION = "1"
"""Bump when ``EdgeBoundaryTranscriber.transcribe``'s behaviour changes - and
bump ``app.stages.transcribe.PROVIDER_VERSION`` in the same commit.

This constant reaches ``capabilities`` and nothing else; only the stage-side
literal reaches ``stage_fingerprint`` (see the module docstring for why the
stage does not read this off ``capabilities``). Bumped alone it therefore
invalidates nothing, which is why
``test_the_capability_version_matches_the_stage_side_constant`` pins the two
equal - bumping either alone fails the gate."""


class EdgeBoundaryTranscriber:
    """Turns TTS-reported word boundaries into ``(word, start_s, end_s)`` triples.

    Free, instant, offline, no GPU - the entire point of capturing boundaries
    at synthesis time (Task 5) rather than re-deriving them from audio with
    ASR, which is what a second, GPU-backed ``Transcriber`` implementation
    would need to do for engines that report nothing.
    """

    capabilities = CapabilityDescriptor(
        provider_id=PROVIDER_ID,
        version=PROVIDER_VERSION,
        cost_model=CostModel.FREE,
        latency_class=LatencyClass.INSTANT,
        offline=True,
        requires_gpu=False,
        vram_mb=None,
        # Only as good as the boundaries an upstream TTS engine reported -
        # edge-tts's own word-boundary events are accurate but not
        # forced-alignment quality, so a respectable middle tier, not the top.
        quality_tier=3,
        # This provider has no language-specific behaviour of its own; it
        # replays whatever boundaries it was handed - "und" (ISO 639-2
        # "undetermined") signals that rather than naming a language, the
        # same reading `providers/story/pasted.py` gives its own
        # caller-controlled seam.
        languages=frozenset({"und"}),
    )

    def transcribe(self, narration: Narration) -> tuple[tuple[str, float, float], ...]:
        """Return ``(word, start_s, end_s)`` triples straight from
        ``narration.boundaries``.

        Never reads ``narration.audio`` - the whole reason this path is free
        is that the timings already exist as ``WordBoundary`` objects;
        decoding audio here would be pure waste, and would blur the boundary
        between this provider and a future ASR-based one that exists
        specifically to do that work when boundaries are unavailable.

        ``end_s`` comes from ``WordBoundary.end_s`` rather than being
        recomputed as ``start_s + duration_s`` here - that property is the
        one definition of "when does this word stop"; recomputing it in a
        second place is exactly the kind of duplication that could drift.

        Raises:
            ProviderError: FATAL, if ``narration.boundaries`` is ``None`` -
                the TTS engine that produced this narration did not report
                word boundaries at all (an audio-only engine, such as Piper
                or ElevenLabs, per ``Narration``'s own docstring). That is
                exactly the case that now requires ASR instead: a
                boundary-consuming transcriber fabricating timings here would
                produce captions that silently drift out of sync, which is
                far harder to notice in review than a stage that failed
                loudly. Pairing an audio-only ``SpeechSynthesizer`` with this
                ``Transcriber`` is a configuration mistake no retry fixes,
                not a transient failure.
        """
        if narration.boundaries is None:
            raise ProviderError(
                "narration carries no word boundaries; the TTS engine that "
                "produced it must be audio-only (e.g. Piper, ElevenLabs) and "
                "reported none - EdgeBoundaryTranscriber only replays "
                "boundaries a TTS engine already captured, so it cannot "
                "recover timings here. Pair an audio-only engine with an "
                "ASR-based Transcriber instead",
                provider_id=PROVIDER_ID,
                kind=ErrorKind.FATAL,
            )
        return tuple((b.text, b.start_s, b.end_s) for b in narration.boundaries)


def make_stage(*, cas: CasStore, settings: Mapping[str, object]) -> Transcribe:
    """Entry point ``story_video:transcribe``.

    Constructs ``EdgeBoundaryTranscriber`` unconditionally - no branch on
    ``settings`` picks between this boundary-replay provider and a future
    ASR-based one. Provider selection from settings is a Phase 2b concern
    this plan deliberately defers; see ``app/stages/transcribe.py``'s module
    docstring for why doing that here, with a fingerprint that read identity
    off the injected object, would make the dispatcher and a worker disagree.

    ``settings`` is accepted, not used: ``Transcribe.settings_keys`` is ``()``
    and ``EdgeBoundaryTranscriber`` takes no configuration at construction
    time, but every entry-point factory shares the same
    ``(*, cas, settings) -> Stage`` shape (``app.registry.build_stage``'s
    contract).
    """
    return Transcribe(cas=cas, transcriber=EdgeBoundaryTranscriber())
