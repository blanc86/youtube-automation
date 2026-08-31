"""``transcribe``: the pipeline's third stage, built on the ``Transcriber`` port.

Depends on ``core.ports.providers.Transcriber`` - never on a concrete provider
class - for the same reason ``synthesize_speech.py`` depends on
``SpeechSynthesizer``: ``ytauto.app`` may not import ``ytauto.providers`` (an
import-linter ``forbidden`` contract), and the Protocol is what lets this
stage be constructed and tested with a fake in place of the real
``EdgeBoundaryTranscriber``. The concrete transcriber is built and injected by
``providers/transcribe/edge_boundary.py``'s ``make_stage``.

This is the stage that proves the free path works end to end: it runs no
speech recognition and takes no GPU lease, because Task 5's
``synthesize_speech`` already captured word-boundary events while edge-tts
streamed. ``Transcribe`` only reshapes what ``boundaries.json`` already holds
into the ``(word, start_s, end_s)`` triples every downstream stage - caption
grouping, the word-by-word highlight, B-roll cut placement - consumes.

**Provider identity is a pair of literal constants, not read off the injected
transcriber's ``capabilities``** - the same choice ``synthesize_speech.py``
makes and for the same reason (see that module's docstring in full): a
factory that later picks a provider from settings (boundary-replay vs. a
future ASR-based ``Transcriber``) could inject different concrete
transcribers into the dispatcher's copy of this stage and a worker's copy,
built from different settings snapshots. Reading identity off the injected
object would make the two compute different fingerprints for what the
dispatcher still thinks is one cached stage. Literal constants here can never
disagree between processes.
"""

from __future__ import annotations

import json

from ytauto.app.stage_support import stage_fingerprint
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.narration import Narration, WordBoundary
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.core.ports.providers import Transcriber
from ytauto.infra.cas.store import CasStore

PROVIDER_ID = "edge-boundary"
"""Literal, fed to ``stage_fingerprint`` - see the module docstring for why
this is not read off ``self._transcriber.capabilities.provider_id``."""

PROVIDER_VERSION = "1"
"""Bump when this stage's *use* of the ``Transcriber`` port changes shape,
**or** when ``EdgeBoundaryTranscriber.transcribe``'s own behaviour changes -
and bump ``providers/transcribe/edge_boundary.py``'s ``PROVIDER_VERSION`` in
the same commit. The two are asserted equal by
``tests/unit/providers/test_edge_boundary.py``, so bumping either alone fails
the gate: only this literal reaches ``stage_fingerprint``, and the provider's
own constant feeds nothing but ``capabilities``, which no fingerprint reads.
See ``synthesize_speech.py``'s identical split for the full account."""


class Transcribe:
    """Turns ``boundaries.json`` into ``word_timings.json`` through an
    injected ``Transcriber``."""

    id = "transcribe"
    version = 1
    depends_on: tuple[str, ...] = ("synthesize_speech",)
    settings_keys: tuple[str, ...] = ()
    """This stage reads no project settings: its entire output is a
    deterministic reshaping of ``synthesize_speech``'s ``boundaries.json``, so
    nothing here varies with a caption colour, a voice, or any other setting.
    The ``Stage`` Protocol declares no default for this, so it must be stated
    explicitly rather than left implicit."""
    gpu_pool = "gpu_compute"
    """Set to the plain default pool despite this stage's own module
    docstring noting it "takes no GPU lease" - that describes real GPU
    *work*, not the governor's own bookkeeping. Every spawn has always taken
    a ``gpu_compute`` lease regardless of whether the stage needs the GPU
    (``Dispatcher._spawn``'s long-standing, deliberately conservative
    behaviour - see its own docstring), so this is not a new lease this
    stage did not have before; it is a required, explicit literal for
    exactly the lease it already took. A dedicated "no lease at all" pool is
    a scheduler-level change, out of this task's scope - see this task's
    report."""

    def __init__(self, *, cas: CasStore, transcriber: Transcriber) -> None:
        self._cas = cas
        self._transcriber = transcriber

    def fingerprint(self, ctx: JobContext) -> str:
        """See the module docstring for why ``provider_id``/``provider_version``
        are the literals above rather than anything read off ``self._transcriber``."""
        return stage_fingerprint(
            self, ctx, provider_id=PROVIDER_ID, provider_version=PROVIDER_VERSION
        )

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        """Read ``boundaries.json``, decode it into ``WordBoundary`` objects,
        and stage the ``(word, start_s, end_s)`` triples the injected
        ``Transcriber`` derives from them.

        ``boundaries.json`` was written by ``synthesize_speech`` as
        ``json.dumps([asdict(b) for b in boundaries])`` - a JSON array of
        objects with exactly ``text``/``start_s``/``duration_s`` - so decoding
        it back is the exact inverse of that ``asdict`` call.

        A ``Narration`` is reconstructed with ``audio=b""`` rather than
        fetched from ``synthesize_speech``'s own ``narration.mp3``. The
        ``Transcriber`` port takes a whole ``Narration`` because its two
        implementations "differ only in which half they read" (see that
        Protocol's own docstring) - but a boundary-consuming implementation
        must never read ``audio`` at all, so there is nothing this stage needs
        to fetch real audio bytes for. Reading and wiring them here would cost
        an unnecessary CAS read on every job for a provider defined never to
        touch them, and would tempt a future change to quietly start reading
        them "since they were already there" - exactly the seam
        ``EdgeBoundaryTranscriber``'s own tests pin against.

        Raises:
            ProviderError: propagated verbatim from the injected
                transcriber's ``transcribe`` - FATAL when
                ``narration.boundaries`` is ``None`` (see
                ``EdgeBoundaryTranscriber.transcribe``'s own docstring for
                why), or whatever kind a future ASR-based ``Transcriber``
                raises for its own failures. This stage trusts the injected
                ``Transcriber`` to raise ``ProviderError`` for everything it
                can classify, never a bare exception; a transcriber that
                violated this would still fail safely, since ``run_stage``
                classifies any non-``ProviderError`` exception FATAL
                regardless.
            KeyError: if a decoded boundary object is missing ``text``,
                ``start_s``, or ``duration_s`` - a malformed or hand-edited
                ``boundaries.json``. ``run_stage`` translates any exception
                raised here into a FATAL worker-protocol error, so this is not
                caught specially.
        """
        boundaries_ref = ctx.input("synthesize_speech", "boundaries.json")
        raw = json.loads(self._cas.read_bytes(boundaries_ref.digest))
        boundaries = tuple(
            WordBoundary(text=item["text"], start_s=item["start_s"], duration_s=item["duration_s"])
            for item in raw
        )
        narration = Narration(audio=b"", boundaries=boundaries)

        triples = self._transcriber.transcribe(narration)

        timings_bytes = json.dumps([list(triple) for triple in triples]).encode("utf-8")
        timings_digest = self._cas.stage_file(timings_bytes, kind="json")

        return StageResult(
            artifacts=(ArtifactRef(name="word_timings.json", kind="json", digest=timings_digest),)
        )
