"""The plugin seams.

Eight Protocols, one per provider family. A new TTS engine or LLM is added by
implementing the relevant Protocol and registering an entry point - with no
change to core/ or app/.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ytauto.core.models.narration import Narration
from ytauto.core.models.visual import VisualCandidate, VisualPlacement
from ytauto.core.ports.capability import CapabilityDescriptor


@runtime_checkable
class StorySource(Protocol):
    """Fetches or imports raw stories."""

    capabilities: CapabilityDescriptor

    def fetch(self, reference: str) -> str:
        """Return raw story text for a URL, file path, or identifier."""


@runtime_checkable
class ScriptGenerator(Protocol):
    """Rewrites a raw story into a narration script."""

    capabilities: CapabilityDescriptor

    def rewrite(self, story: str, *, style: str) -> str:
        """Return the rewritten script."""


@runtime_checkable
class SpeechSynthesizer(Protocol):
    """Turns script text into narration audio."""

    capabilities: CapabilityDescriptor

    def synthesize(self, text: str, *, voice: str) -> Narration:
        """Return the synthesised audio, with word boundaries if this engine
        emits them.

        Returning ``Narration`` rather than bare bytes is what makes the free
        captioning path possible: an engine that already knows where each word
        starts (edge-tts reports offset+duration per word as it streams) has
        given away for free exactly what ASR would otherwise need a GPU lease
        to recover. An engine that reports nothing sets
        ``Narration.boundaries`` to None, which is the signal a
        boundary-consuming transcriber refuses on rather than guessing.
        """


@runtime_checkable
class Transcriber(Protocol):
    """Produces word-level timings for narration audio.

    Two implementations exist by design: one consuming TTS word-boundary
    metadata (free, instant, no GPU) and one running ASR (universal, needs a
    GPU lease). Same port, very different cost.
    """

    capabilities: CapabilityDescriptor

    def transcribe(self, narration: Narration) -> tuple[tuple[str, float, float], ...]:
        """Return (word, start_seconds, end_seconds) triples.

        Takes the whole ``Narration``, not just its bytes, so that the two
        implementations differ only in which half they read: the free one
        consumes ``boundaries`` and raises when it is None; the ASR one
        ignores ``boundaries`` and decodes ``audio``. A bytes-only signature
        would force the boundaries to travel beside the port as a second
        argument every caller had to remember to pass.
        """


@runtime_checkable
class VisualStrategy(Protocol):
    """Populates a timeline's visual segments.

    Widened for Task 10, the same way ``SpeechSynthesizer``/``Transcriber``
    were widened in Task 3 (see that task's design note): the original
    ``plan(duration_s: float, *, seed: int) -> tuple[str, ...]`` could not
    express what ``select_broll`` actually needs. A timeline's segments are
    independently sized (``core.pipeline.timeline.Segment``), so a single
    ``duration_s`` cannot stand in for all of them; a bare tuple of strings
    has nowhere to carry an in-point; and there was no parameter at all for
    the candidate library a selection strategy draws from - it would have had
    to be smuggled in at construction time instead, which ``make_stage``'s
    "construct the provider unconditionally, no branch on settings" rule
    does not comfortably accommodate for data that is itself a per-job
    ``settings`` value (the manifest digest). There are zero implementations
    of the old shape anywhere in the codebase, so this costs nothing today.
    """

    capabilities: CapabilityDescriptor

    def plan(
        self,
        segment_durations: Sequence[float],
        candidates: Sequence[VisualCandidate],
        *,
        seed: int,
    ) -> tuple[VisualPlacement, ...]:
        """Return one ``VisualPlacement`` per entry in ``segment_durations``,
        in the same order, each drawn from ``candidates``.

        Implementations are free to repeat a candidate once every eligible
        one has been used, but must never choose a candidate shorter than the
        segment it would fill.
        """


@runtime_checkable
class ImageGenerator(Protocol):
    """Generates a still image from a prompt."""

    capabilities: CapabilityDescriptor

    def generate(self, prompt: str, *, width: int, height: int) -> bytes:
        """Return encoded image bytes."""


@runtime_checkable
class ThumbnailRenderer(Protocol):
    """Composes a video thumbnail."""

    capabilities: CapabilityDescriptor

    def render(self, title: str, *, background: bytes) -> bytes:
        """Return encoded thumbnail image bytes."""


@runtime_checkable
class Publisher(Protocol):
    """Reserved seam - no implementation ships.

    The YouTube Data API bills an upload at 1,600 quota units against a
    10,000/day default, capping roughly 6 uploads/day regardless of how many
    videos are rendered. Export-to-file is the supported path; this exists so
    publishing can be added later without structural change.
    """

    capabilities: CapabilityDescriptor

    def publish(self, video_path: str, *, title: str, description: str) -> str:
        """Return the published video's identifier."""
