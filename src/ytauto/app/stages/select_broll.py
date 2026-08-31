"""``select_broll``: the pipeline's fifth stage, built on the ``VisualStrategy`` port.

Depends on ``core.ports.providers.VisualStrategy``, never on a concrete
provider class - the same split ``transcribe.py`` and ``synthesize_speech.py``
use, and for the same reason: ``ytauto.app`` may not import
``ytauto.providers`` (an import-linter ``forbidden`` contract), and the
Protocol is what lets this stage be constructed and tested with a fake in
place of the real ``LibraryVisualStrategy``. The concrete strategy is built
and injected by ``providers/visual/library.py``'s ``make_stage``.

This is the stage that decides which B-roll clip fills each gap in the
timeline. It reads two things and nothing else: ``timeline.json`` (Task 7),
for the ordered list of segment spans to fill, and the B-roll manifest (Task
9) - a CAS blob whose digest arrives as a plain ``ctx.settings`` value, never
a SQL query, because this stage runs in a worker and ``CasStore.read_bytes``
is the worker-safe way to reach it. Everything about *which* clip goes where -
the no-repeat-until-exhausted draw, the duration filter, the seeded in-point -
is the injected ``VisualStrategy``'s job; this stage only shuttles JSON in and
out and reshapes it at the two boundaries.

**Provider identity is a pair of literal constants, not read off the injected
strategy's ``capabilities``** - see ``transcribe.py``'s module docstring for
the full argument: a factory that later picks a provider from settings could
inject different concrete strategies into the dispatcher's copy of this stage
and a worker's copy, built from different settings snapshots. Literal
constants here can never disagree between processes.
"""

from __future__ import annotations

import json

from ytauto.app.stage_support import stage_fingerprint
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash
from ytauto.core.models.visual import VisualCandidate
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.core.ports.providers import VisualStrategy
from ytauto.infra.cas.store import CasStore

PROVIDER_ID = "library"
"""Literal, fed to ``stage_fingerprint`` - see the module docstring for why
this is not read off ``self._visual_strategy.capabilities.provider_id``."""

PROVIDER_VERSION = "1"
"""Bump when this stage's *use* of the ``VisualStrategy`` port changes shape,
**or** when ``LibraryVisualStrategy.plan``'s selection behaviour changes - and
bump ``providers/visual/library.py``'s ``PROVIDER_VERSION`` in the same
commit. The two are asserted equal by
``tests/unit/providers/test_library_visual.py``, so bumping either alone fails
the gate: only this literal reaches ``stage_fingerprint``, and the provider's
own constant feeds nothing but ``capabilities``, which no fingerprint reads -
so changing the selection algorithm and bumping only the provider's constant
would have kept serving the old edit from cache. See ``transcribe.py``'s
identical split."""


def _as_int(value: object) -> int:
    """Narrow one ``ctx.settings`` value to ``int``.

    ``bool`` is a subclass of ``int`` in Python, so it is excluded explicitly
    - a stray ``True``/``False`` must not silently act as a seed of ``1``/``0``.

    Raises:
        TypeError: ``value`` is not an ``int``.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"expected an int, got {type(value).__name__}")
    return value


def _as_digest(value: object) -> ContentHash:
    """Narrow one ``ctx.settings`` value to a ``ContentHash``.

    ``broll_manifest_digest`` reaches this stage as a plain string in
    ``ctx.settings`` (Task 9 writes the manifest and hands back its digest;
    nothing about that digest is validated again until ``CasStore.read_bytes``
    resolves it to a path - malformed digests fail there, not here).

    Raises:
        TypeError: ``value`` is not a ``str``.
    """
    if not isinstance(value, str):
        raise TypeError(f"expected a str digest, got {type(value).__name__}")
    return ContentHash(value)


class SelectBroll:
    """Turns ``timeline.json`` and the B-roll manifest into ``segments.json``
    through an injected ``VisualStrategy``."""

    id = "select_broll"
    version = 1
    depends_on: tuple[str, ...] = ("plan_timeline",)
    settings_keys: tuple[str, ...] = ("broll_manifest_digest", "seed")
    gpu_pool = "gpu_compute"
    """No GPU work at all; the plain default pool - see
    ``core.pipeline.stage.Stage.gpu_pool``'s own docstring for why this is a
    required, explicit literal rather than an implicit fallback."""

    def __init__(self, *, cas: CasStore, visual_strategy: VisualStrategy) -> None:
        self._cas = cas
        self._visual_strategy = visual_strategy

    def fingerprint(self, ctx: JobContext) -> str:
        """See the module docstring for why ``provider_id``/``provider_version``
        are the literals above rather than anything read off
        ``self._visual_strategy``."""
        return stage_fingerprint(
            self, ctx, provider_id=PROVIDER_ID, provider_version=PROVIDER_VERSION
        )

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        """Read ``timeline.json`` and the B-roll manifest, delegate selection
        to the injected ``VisualStrategy``, and stage ``segments.json``.

        ``timeline.json`` was written by ``plan_timeline`` as
        ``json.dumps(asdict(timeline))``, so ``timeline["segments"]`` is a
        JSON array of ``{"start_s": ..., "end_s": ...}`` objects - each
        segment's duration is derived here as ``end_s - start_s``.

        The manifest was written by ``BrollLibrary.write_manifest`` (Task 9)
        as a JSON array of ``{"clip_id", "duration_s", "source_width",
        "source_height", "normalised_landscape_digest",
        "normalised_vertical_digest"}`` objects; only ``clip_id`` and
        ``duration_s`` are read here; the two digests are resolved to a
        canvas by the two compose stages, never by this one, so this
        selection serves both.

        ``segments.json`` is staged as a JSON array of
        ``{"clip_id", "in_point_s", "duration_s"}`` objects, one per input
        segment, in order - never a digest, per this task's brief, so one
        selection can serve both compose stages.

        Raises:
            ProviderError: propagated verbatim from the injected strategy's
                ``plan`` - FATAL when the library has no clips at all, or no
                clip is long enough for some segment (see
                ``LibraryVisualStrategy.plan``'s own docstring). This stage
                trusts the injected ``VisualStrategy`` to raise
                ``ProviderError`` for everything it can classify.
            KeyError: ``ctx.settings`` is missing ``broll_manifest_digest`` or
                ``seed``, or a decoded timeline/manifest object is missing an
                expected field - a malformed or hand-edited upstream artifact.
                ``run_stage`` translates any exception raised here into a
                FATAL worker-protocol error, so this is not caught specially.
            TypeError: a settings value is present but the wrong type.
        """
        timeline_ref = ctx.input("plan_timeline", "timeline.json")
        timeline_raw = json.loads(self._cas.read_bytes(timeline_ref.digest))
        segment_durations = tuple(
            float(segment["end_s"]) - float(segment["start_s"])
            for segment in timeline_raw["segments"]
        )

        manifest_digest = _as_digest(ctx.settings["broll_manifest_digest"])
        manifest_raw = json.loads(self._cas.read_bytes(manifest_digest))
        candidates = tuple(
            VisualCandidate(asset_id=str(entry["clip_id"]), duration_s=float(entry["duration_s"]))
            for entry in manifest_raw
        )

        seed = _as_int(ctx.settings["seed"])

        placements = self._visual_strategy.plan(segment_durations, candidates, seed=seed)

        segments_payload = [
            {
                "clip_id": placement.asset_id,
                "in_point_s": placement.in_point_s,
                "duration_s": placement.duration_s,
            }
            for placement in placements
        ]
        segments_bytes = json.dumps(segments_payload).encode("utf-8")
        segments_digest = self._cas.stage_file(segments_bytes, kind="json")

        return StageResult(
            artifacts=(ArtifactRef(name="segments.json", kind="json", digest=segments_digest),)
        )
