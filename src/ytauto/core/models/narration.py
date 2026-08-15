"""Synthesised speech and its word timings.

The two ports that carry these (``SpeechSynthesizer`` and ``Transcriber``)
are one seam, not two: an engine that already reports word boundaries has
given away for free exactly what ASR would otherwise burn a GPU lease to
recover. Modelling boundaries as part of the narration - rather than making
every caller re-run ASR over raw bytes - is what lets the free path exist at
all.
"""

from __future__ import annotations

from dataclasses import dataclass

from ytauto.core.errors import ValidationError


@dataclass(frozen=True)
class WordBoundary:
    """One word's span, as reported by a TTS engine that emits boundaries.

    Durations, not end times, because that is what the engines report (edge-tts
    emits offset+duration per word); deriving an end is trivial, and storing
    both would let the two disagree.

    Raises:
        ValidationError: if ``text`` is empty or ``duration_s`` is negative.
    """

    text: str
    start_s: float
    duration_s: float

    def __post_init__(self) -> None:
        if not self.text:
            raise ValidationError("WordBoundary.text must not be empty")
        if self.duration_s < 0:
            raise ValidationError(
                f"WordBoundary.duration_s must not be negative: {self.duration_s}"
            )

    @property
    def end_s(self) -> float:
        """When this word stops, in seconds from the start of the narration."""
        return self.start_s + self.duration_s


@dataclass(frozen=True)
class Narration:
    """Synthesised speech, plus word boundaries when the engine emits them.

    ``boundaries`` is None for engines that produce audio only (Piper,
    ElevenLabs). That is precisely the case that forces ASR, so a
    boundary-consuming transcriber must refuse it loudly rather than
    fabricating timings: captions that drift out of sync are far harder to
    notice in review than a stage that failed.

    ``audio`` is the encoded bytes rather than a CAS digest because a
    synthesizer hands its output straight to a transcriber inside one stage;
    only what the *stage* produces becomes an artifact.
    """

    audio: bytes
    boundaries: tuple[WordBoundary, ...] | None
