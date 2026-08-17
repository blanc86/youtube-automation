"""``LibraryVisualStrategy``, and the entry-point factory that wires it into
``SelectBroll``.

Mirrors ``providers/transcribe/edge_boundary.py``'s split: ``LibraryVisualStrategy``
(the ``VisualStrategy`` port implementation) lives here under ``providers/``;
the stage that consumes it, ``SelectBroll``, lives in
``ytauto.app.stages.select_broll`` typed against the ``VisualStrategy``
Protocol, never against this concrete class, because ``ytauto.app`` may not
import ``ytauto.providers`` (an import-linter ``forbidden`` contract).
``make_stage`` below is the one thing allowed to import both sides of that
boundary, resolved by ``app/registry.py`` dynamically through
``importlib.metadata`` entry points rather than a static import - invisible to
import-linter.

This is the whole selection algorithm for Phase 2a's only ``VisualStrategy``
implementation: pick a B-roll clip for each segment of the timeline, drawing
without replacement from a seeded shuffle of the library until it is
exhausted, then reshuffling and starting again. See ``plan``'s own docstring
for the exact rules and why each one exists.

**Unlike ``SelectBroll.fingerprint``, this module's own
``PROVIDER_ID``/``PROVIDER_VERSION`` constants are never read by the stage.**
The stage carries its own literal copies (see its module docstring). The
constants stay here anyway, on ``LibraryVisualStrategy.capabilities``, because
that descriptor is still honest metadata other callers (a future cost policy,
a provider picker) may legitimately consult - it is only the *fingerprint*
that must not.
"""

from __future__ import annotations

import random
from collections.abc import Mapping, Sequence

from ytauto.app.stages.select_broll import SelectBroll
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.visual import VisualCandidate, VisualPlacement
from ytauto.core.ports.capability import CapabilityDescriptor, CostModel, LatencyClass
from ytauto.infra.cas.store import CasStore

PROVIDER_ID = "library"
PROVIDER_VERSION = "1"
"""Bump when ``LibraryVisualStrategy.plan``'s selection behaviour changes -
and bump ``app.stages.select_broll.PROVIDER_VERSION`` in the same commit.

This constant reaches ``capabilities`` and nothing else; only the stage-side
literal reaches ``stage_fingerprint`` (see the module docstring for why the
stage does not read this off ``capabilities``). Bumped alone it therefore
invalidates nothing - change ``plan``'s algorithm, bump this dutifully, and
every project keeps its cached ``segments.json`` from the old algorithm. That
is the concrete case the whole-branch review named, which is why
``test_the_capability_version_matches_the_stage_side_constant`` pins the two
equal: bumping either alone fails the gate."""


class LibraryVisualStrategy:
    """Draws B-roll clips from the ingest library's manifest for each segment
    of a timeline.

    Free, instant, offline: selection is pure bookkeeping over data the
    manifest already carries, with no network call and no model to load.
    """

    capabilities = CapabilityDescriptor(
        provider_id=PROVIDER_ID,
        version=PROVIDER_VERSION,
        cost_model=CostModel.FREE,
        latency_class=LatencyClass.INSTANT,
        offline=True,
        requires_gpu=False,
        vram_mb=None,
        # A random draw from a curated library is a reasonable default edit,
        # but nowhere near a strategy that reasons about shot content or
        # pacing - the middle tier, same as EdgeBoundaryTranscriber's.
        quality_tier=3,
        languages=frozenset({"und"}),
    )

    def plan(
        self,
        segment_durations: Sequence[float],
        candidates: Sequence[VisualCandidate],
        *,
        seed: int,
    ) -> tuple[VisualPlacement, ...]:
        """Return one ``VisualPlacement`` per entry in ``segment_durations``,
        in order, drawn from ``candidates``.

        The rules, per this task's brief:

        1. **No clip repeats until the library is exhausted.** A pool starts
           empty and is (re)filled with a fresh ``random.Random(seed)``
           shuffle of ``candidates`` whenever it cannot satisfy the current
           segment (see rule 2) - which includes the very first segment,
           since an empty pool can never satisfy anything. Once a candidate
           is drawn for a segment it is removed from the pool, so within one
           fill every candidate can be drawn at most once; repeats become
           possible only after a refill, which is exactly when the library
           has been exhausted for that segment's duration requirement.

           A refill alone is not enough to guarantee this, though: a fresh
           shuffle of the whole library can, by chance, place the clip that
           was just drawn (the one whose exhaustion triggered the refill)
           back at the front - an immediate, visible back-to-back repeat
           across the boundary, which is exactly the failure this rule
           exists to prevent. So immediately after a refill, if the first
           eligible candidate is the immediately-prior selection *and* more
           than one eligible candidate exists, it is rotated to the back
           instead of being drawn again right away. Guarded on "more than
           one eligible candidate": with only one clip either globally or
           after the duration filter, there is no alternative and repeating
           it is correct, not a bug.
        2. **A clip shorter than its segment is never chosen for it.** Before
           drawing, the pool is filtered to candidates whose ``duration_s``
           is at least the segment's own duration. A candidate that fails
           this filter is left untouched in the pool - not removed - so it
           remains available to a later, shorter segment. If filtering
           empties the *eligible* set (even though the raw pool is
           non-empty: every remaining candidate happens to be too short),
           that counts as exhaustion too, and triggers the same refill as
           rule 1 - a clip already drawn earlier in this fill may be the
           only one long enough, and repeating it beats failing the segment
           outright.
        3. **``in_point_s`` is a seeded random offset.** Drawn from
           ``rng.uniform(0.0, candidate.duration_s - segment_duration)`` when
           the candidate is strictly longer than the segment; exactly ``0.0``,
           with no draw at all, when it matches exactly - both so the
           in-point is never negative and so the same seed always reproduces
           the same edit.

        Raises:
            ProviderError: FATAL, if ``candidates`` is empty and at least one
                segment needs filling - no retry can conjure footage, so this
                is never retryable. FATAL, if some segment's duration exceeds
                every candidate's ``duration_s`` - the message names the
                segment's duration and the longest clip actually available,
                so the fix (add a longer clip) is legible from the error
                alone rather than a bare "selection failed".
        """
        if not segment_durations:
            return ()
        if not candidates:
            raise ProviderError(
                "the B-roll library has no clips; add at least one clip with "
                "`ytauto broll add` before selecting B-roll for this project",
                provider_id=PROVIDER_ID,
                kind=ErrorKind.FATAL,
            )

        rng = random.Random(seed)
        pool: list[VisualCandidate] = []
        placements: list[VisualPlacement] = []
        previous_asset_id: str | None = None

        for duration_s in segment_durations:
            eligible = [c for c in pool if c.duration_s >= duration_s]
            refilled = False
            if not eligible:
                pool = list(candidates)
                rng.shuffle(pool)
                eligible = [c for c in pool if c.duration_s >= duration_s]
                refilled = True
            if not eligible:
                longest = max(c.duration_s for c in candidates)
                raise ProviderError(
                    f"no clip in the B-roll library is at least {duration_s:.2f}s "
                    f"long, but a segment of that length needs one; the longest "
                    f"clip available is {longest:.2f}s - add a longer clip with "
                    "`ytauto broll add`",
                    provider_id=PROVIDER_ID,
                    kind=ErrorKind.FATAL,
                )

            if (
                refilled
                and previous_asset_id is not None
                and eligible[0].asset_id == previous_asset_id
                and len(eligible) > 1
            ):
                # A fresh shuffle can, by chance, put the clip that just
                # exhausted the previous pass back at the front - rotate past
                # it rather than draw it again immediately. See rule 1 above.
                eligible = eligible[1:] + eligible[:1]

            chosen = eligible[0]
            pool.remove(chosen)
            previous_asset_id = chosen.asset_id

            if chosen.duration_s > duration_s:
                in_point_s = rng.uniform(0.0, chosen.duration_s - duration_s)
            else:
                in_point_s = 0.0

            placements.append(
                VisualPlacement(
                    asset_id=chosen.asset_id, in_point_s=in_point_s, duration_s=duration_s
                )
            )

        return tuple(placements)


def make_stage(*, cas: CasStore, settings: Mapping[str, object]) -> SelectBroll:
    """Entry point ``story_video:select_broll``.

    Constructs ``LibraryVisualStrategy`` unconditionally - no branch on
    ``settings`` picks between this library-backed provider and a future
    generative one. Provider selection from settings is a later concern this
    plan deliberately defers; see ``app/stages/select_broll.py``'s module
    docstring for why doing that here, with a fingerprint that read identity
    off the injected object, would make the dispatcher and a worker disagree.

    ``settings`` is accepted, not used at construction time:
    ``LibraryVisualStrategy`` takes no configuration - it reads
    ``broll_manifest_digest`` and ``seed`` through ``ctx.settings`` at run
    time instead, via ``SelectBroll.run`` - but every entry-point factory
    shares the same ``(*, cas, settings) -> Stage`` shape
    (``app.registry.build_stage``'s contract).
    """
    return SelectBroll(cas=cas, visual_strategy=LibraryVisualStrategy())
