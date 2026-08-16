"""``synthesize_speech``: the pipeline's second stage, built on the
``SpeechSynthesizer`` port.

Depends on ``core.ports.providers.SpeechSynthesizer`` - never on a concrete
provider class - for the same reason ``ingest_story.py`` depends on
``StorySource``: ``ytauto.app`` may not import ``ytauto.providers`` (an
import-linter ``forbidden`` contract), and the Protocol is what lets this
stage be constructed and tested with a fake in place of the real
``EdgeTtsSynthesizer``. The concrete synthesizer is built and injected by
``providers/tts/edge.py``'s ``make_stage``.

**Provider identity is a pair of literal constants, not read off the
injected synthesizer's ``capabilities`` - deliberately unlike
``IngestStory.fingerprint``, which reads ``provider_id``/``provider_version``
off ``self._source.capabilities``.** That was safe there because
``providers/story/pasted.py``'s ``make_stage`` injects ``PastedStorySource``
unconditionally, so the dispatcher and a worker always build the same source
from any settings they are handed and therefore always compute the same
fingerprint. The moment a factory picks its provider *from settings* -
``edge-tts`` vs. Piper vs. ElevenLabs, a Phase 2b concern this plan
deliberately defers - the dispatcher (built once per process, from whatever
settings its caller passed) and a worker (rebuilt per job, from that job's
real settings) can inject *different* concrete synthesizers from *different*
settings snapshots, and reading identity off the injected object would make
the two processes compute different fingerprints for what the dispatcher
still thinks is one cached stage. ``app/worker.py``'s
``_fingerprint_disagreement`` catches that loudly rather than silently
poisoning the cache, but it fails every job the stage is given, not some.
Literal constants here can never disagree between processes, so this stage
stays safe even after a future task adds that branch - it is the conservative
choice named explicitly in this task's brief.
"""

from __future__ import annotations

import json
from dataclasses import asdict

from ytauto.app.stage_support import stage_fingerprint
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.core.ports.providers import SpeechSynthesizer
from ytauto.infra.cas.store import CasStore

PROVIDER_ID = "edge-tts"
"""Literal, fed to ``stage_fingerprint`` - see the module docstring for why
this is not read off ``self._synth.capabilities.provider_id``."""

PROVIDER_VERSION = "1"
"""Bump when this stage's *use* of the synthesizer port changes shape (for
example, a new field read out of ``Narration``) - independent of
``providers/tts/edge.py``'s own ``PROVIDER_VERSION``, which tracks
``EdgeTtsSynthesizer.synthesize``'s own behaviour. The two happen to share a
value today; nothing keeps them in sync, by design - literal constants exist
here precisely so this module never has to import the provider that owns the
other one."""


class SynthesizeSpeech:
    """Turns ``story.txt`` into ``narration.mp3`` and ``boundaries.json``
    through an injected ``SpeechSynthesizer``."""

    id = "synthesize_speech"
    version = 1
    depends_on: tuple[str, ...] = ("ingest_story",)
    settings_keys: tuple[str, ...] = ("voice", "rate")

    def __init__(self, *, cas: CasStore, synthesizer: SpeechSynthesizer) -> None:
        self._cas = cas
        self._synth = synthesizer

    def fingerprint(self, ctx: JobContext) -> str:
        """See the module docstring for why ``provider_id``/``provider_version``
        are the literals above rather than anything read off ``self._synth``."""
        return stage_fingerprint(
            self, ctx, provider_id=PROVIDER_ID, provider_version=PROVIDER_VERSION
        )

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        """Read ``story.txt``, synthesize it, and stage the audio plus its
        word-level timings.

        Raises:
            ProviderError: FATAL, if the injected synthesizer reports no word
                boundaries at all (``Narration.boundaries is None`` - some
                ``SpeechSynthesizer`` implementations are audio-only, per that
                port's own docstring). This stage's entire output includes
                ``boundaries.json``; a provider that cannot supply boundaries
                is a configuration mistake no retry fixes, not a transient
                failure.
            ProviderError: propagated verbatim from the injected
                synthesizer's ``synthesize`` for any other failure - retried
                or not per that error's own ``kind``. This stage trusts the
                injected ``SpeechSynthesizer`` to raise ``ProviderError`` for
                everything it can classify, never a bare exception;
                ``EdgeTtsSynthesizer.synthesize`` holds up that end (a defect
                where it briefly did not - a malformed voice string leaking a
                bare ``ValueError`` around its own mapping - was found in
                Task 5's review and fixed in ``providers/tts/edge.py``). A
                synthesizer that violated this would still fail safely:
                ``run_stage`` classifies any non-``ProviderError`` exception
                FATAL regardless.
            KeyError: if ``ctx.settings`` carries no ``"voice"`` - a job
                enqueued with no voice selected. ``run_stage`` translates any
                exception raised here into a FATAL worker-protocol error, so
                this is not caught specially.
        """
        story = ctx.input("ingest_story", "story.txt")
        text = self._cas.read_bytes(story.digest).decode("utf-8")
        voice = str(ctx.settings["voice"])

        narration = self._synth.synthesize(text, voice=voice)
        if narration.boundaries is None:
            raise ProviderError(
                "synthesizer produced audio but no word boundaries; this stage "
                "cannot emit boundaries.json without them - pairing it with an "
                "audio-only SpeechSynthesizer is a configuration mistake",
                provider_id=PROVIDER_ID,
                kind=ErrorKind.FATAL,
            )

        audio_digest = self._cas.stage_file(narration.audio, kind="audio")
        boundaries_bytes = json.dumps([asdict(b) for b in narration.boundaries]).encode("utf-8")
        boundaries_digest = self._cas.stage_file(boundaries_bytes, kind="json")

        return StageResult(
            artifacts=(
                ArtifactRef(name="narration.mp3", kind="audio", digest=audio_digest),
                ArtifactRef(name="boundaries.json", kind="json", digest=boundaries_digest),
            )
        )
