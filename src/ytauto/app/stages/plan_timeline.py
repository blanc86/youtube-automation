"""``plan_timeline``: the pipeline's fourth stage, wrapping the pure
``plan_timeline`` function in ``core.pipeline.timeline``.

Unlike every earlier stage, this one has **no provider**. ``core.pipeline.
timeline.plan_timeline`` is a pure function of its own arguments, not a port
implementation - there is no ``Protocol`` in ``core.ports.providers`` for it
to satisfy, no concrete class under ``providers/`` to inject, and therefore
nothing for ``__init__`` to receive beyond the ``CasStore`` every stage
needs to stage its own output. ``make_stage`` (the entry point for
``story_video:plan_timeline``) lives in this module too, rather than split
across a ``providers/`` package the way ``ingest_story``/``synthesize_speech``/
``transcribe`` are - there is no concrete-vs-Protocol seam here for a
factory to stand on both sides of.

**Provider identity is still a literal pair, not "none" or an empty
string.** ``stage_fingerprint`` requires a ``provider_id``/``provider_version``
for every stage, whether or not it has a real provider (see
``app.stage_support``'s own docstring: the fingerprint is the whole caching
mechanism, and it always needs *something* stable to hash). ``"pure"`` says
plainly that there is no provider, rather than reusing another stage's
literal or inventing a name that would misleadingly suggest one exists.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict

from ytauto.app.stage_support import stage_fingerprint
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.core.pipeline.timeline import plan_timeline
from ytauto.infra.cas.store import CasStore

PROVIDER_ID = "pure"
"""Literal, fed to ``stage_fingerprint``. There is no provider to read
identity off (see the module docstring), so this states that plainly rather
than defaulting to something that implies one exists."""

PROVIDER_VERSION = "1"
"""Bump whenever ``core.pipeline.timeline.plan_timeline``'s algorithm
changes - the grouping rule, the segmentation rule, or the punctuation set
treated as sentence-ending. A change here invalidates every cached
``timeline.json``, which is correct: a different algorithm is a different
edit, even given the exact same ``word_timings.json`` and settings."""


def _as_int(value: object) -> int:
    """Narrow one ``ctx.settings`` value to ``int``.

    A local, deliberate duplicate of ``core.pipeline.timeline``'s own
    private ``_require_int`` rather than an import of it - that function is
    prefixed ``_`` precisely to signal it is not meant to be reached into
    from outside its module. ``bool`` is a subclass of ``int`` in Python, so
    it is excluded explicitly: a stray ``True``/``False`` must not silently
    act as a word-count cap of ``1``/``0``.

    Raises:
        TypeError: ``value`` is not an ``int``.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"expected an int, got {type(value).__name__}")
    return value


def _as_float(value: object) -> float:
    """Narrow one ``ctx.settings`` value to ``float``. An ``int`` is
    accepted and widened, mirroring ``core.pipeline.timeline``'s own
    ``_require_float`` - see ``_as_int`` for why this is a local duplicate
    rather than an import.

    Raises:
        TypeError: ``value`` is neither ``float`` nor ``int``.
    """
    if isinstance(value, bool):
        raise TypeError("expected a float, got bool")
    if isinstance(value, int):
        return float(value)
    if not isinstance(value, float):
        raise TypeError(f"expected a float, got {type(value).__name__}")
    return value


class PlanTimeline:
    """Turns ``word_timings.json`` into ``timeline.json`` by calling the pure
    ``plan_timeline`` function.

    ``audio_duration_s`` - the third argument ``plan_timeline`` needs beyond
    ``word_timings`` and ``template`` - is derived here as the last word's
    own ``end_s`` (``0.0`` if there are no words at all), because
    ``word_timings.json`` is the only artifact this stage reads (Step 9 of
    this task's brief is explicit that ``run`` reads *only*
    ``word_timings.json``). This is a real gap, not a rounding matter:
    ``core.pipeline.timeline``'s own docstring and its pinned
    ``test_audio_longer_than_the_last_word_still_tiles_to_the_end`` exist
    precisely because edge-tts pads trailing silence past the last word, and
    that padding is real audio duration this stage currently has no way to
    see - there is no ffprobe call on ``narration.mp3`` anywhere in this
    pipeline yet, and no other artifact or ``JobContext`` field carries a
    real duration. The pure function fully supports a longer
    ``audio_duration_s`` than the last word's end; this stage just cannot
    currently supply one. Flagged in this task's report as a candidate for a
    later task (most naturally, an ffprobe-backed duration wired in wherever
    the pipeline first has real audio bytes on disk) rather than fixed here,
    since Step 9 scopes this stage to reading ``word_timings.json`` alone.
    """

    id = "plan_timeline"
    version = 1
    depends_on: tuple[str, ...] = ("transcribe",)
    settings_keys: tuple[str, ...] = (
        "words_per_group_min",
        "words_per_group_max",
        "segment_seconds_min",
        "segment_seconds_max",
        "seed",
    )
    gpu_pool = "gpu_compute"
    """No GPU work at all; the plain default pool - see
    ``core.pipeline.stage.Stage.gpu_pool``'s own docstring for why this is a
    required, explicit literal rather than an implicit fallback."""

    def __init__(self, *, cas: CasStore) -> None:
        self._cas = cas

    def fingerprint(self, ctx: JobContext) -> str:
        """See the module docstring for why ``provider_id``/``provider_version``
        are the ``"pure"``/``"1"`` literals above rather than anything read
        off an injected object - there is no injected object."""
        return stage_fingerprint(
            self, ctx, provider_id=PROVIDER_ID, provider_version=PROVIDER_VERSION
        )

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        """Read ``word_timings.json``, plan the edit, and stage ``timeline.json``.

        ``word_timings.json`` was written by ``transcribe`` as
        ``json.dumps([list(triple) for triple in triples])`` - a JSON array
        of ``[text, start_s, end_s]`` triples - confirmed against
        ``src/ytauto/app/stages/transcribe.py`` before writing this reader,
        per this task's brief. Decoding it back is the exact inverse of that
        call.

        ``timeline.json`` is staged as ``json.dumps(asdict(timeline))``, per
        Step 9: ``asdict`` recurses through ``Timeline``'s nested
        ``CaptionGroup``/``Segment`` tuples, so ``groups`` and ``segments``
        each serialise as a JSON array of objects (``{"start_s": ...,
        "end_s": ..., "words": [...]}`` / ``{"start_s": ..., "end_s": ...}``),
        not as a flat array of tuples.

        Raises:
            KeyError: ``ctx.settings`` is missing one of ``settings_keys`` -
                a job enqueued without a complete template. ``run_stage``
                translates any exception raised here into a FATAL
                worker-protocol error, so this is not caught specially.
            TypeError: ``core.pipeline.timeline.plan_timeline`` rejects a
                settings value of the wrong type (see its own docstring).
        """
        timings_ref = ctx.input("transcribe", "word_timings.json")
        raw = json.loads(self._cas.read_bytes(timings_ref.digest))
        word_timings = tuple((str(item[0]), float(item[1]), float(item[2])) for item in raw)

        # See the class docstring for why this is the last word's end rather
        # than a true probed audio duration.
        audio_duration_s = word_timings[-1][2] if word_timings else 0.0

        template: Mapping[str, object] = {
            "words_per_group_min": _as_int(ctx.settings["words_per_group_min"]),
            "words_per_group_max": _as_int(ctx.settings["words_per_group_max"]),
            "segment_seconds_min": _as_float(ctx.settings["segment_seconds_min"]),
            "segment_seconds_max": _as_float(ctx.settings["segment_seconds_max"]),
        }
        seed = _as_int(ctx.settings["seed"])

        timeline = plan_timeline(word_timings, audio_duration_s, template, seed)

        timeline_bytes = json.dumps(asdict(timeline)).encode("utf-8")
        timeline_digest = self._cas.stage_file(timeline_bytes, kind="json")

        return StageResult(
            artifacts=(ArtifactRef(name="timeline.json", kind="json", digest=timeline_digest),)
        )


def make_stage(*, cas: CasStore, settings: Mapping[str, object]) -> PlanTimeline:
    """Entry point ``story_video:plan_timeline``.

    ``settings`` is accepted, not forwarded - every entry-point factory
    shares the ``(*, cas, settings)`` shape (``app.registry.build_stage``'s
    contract) - but unused: this stage has no provider to pick, so there is
    no settings-dependent construction-time decision the way
    ``providers/tts/edge.py``'s ``make_stage`` has one deferred to Phase 2b.
    ``PlanTimeline`` reads its settings at run time instead, through
    ``ctx.settings`` (see ``PlanTimeline.run``).
    """
    return PlanTimeline(cas=cas)
