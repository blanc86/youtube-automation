"""``LibraryVisualStrategy``, and ``make_stage``'s wiring of it into
``SelectBroll``.

``SelectBroll`` itself is typed against the ``VisualStrategy`` Protocol and
lives in ``ytauto.app.stages.select_broll`` - its behavioural tests are in
``tests/unit/app/stages/test_select_broll.py``. What belongs here is
everything specific to this concrete provider: the selection algorithm
itself (no-repeat-until-exhausted, the duration filter, the seeded in-point,
both FATAL cases), that its ``CapabilityDescriptor`` is honest, that it
actually satisfies the ``VisualStrategy`` Protocol it claims to, and that
``make_stage`` - the one function allowed to import both sides of the
``app``/``providers`` boundary - wires the two together correctly.

The five tests in this task's brief (Step 1) are reproduced verbatim below,
against ``_select``/``_clips``/``_clip`` helpers this file defines - the
brief names those helpers but does not itself define them, since Step 1 only
shows the assertions they must satisfy.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ytauto.app.stages.select_broll import SelectBroll
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.visual import VisualCandidate
from ytauto.core.ports.capability import CostModel
from ytauto.core.ports.providers import VisualStrategy
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations
from ytauto.providers.visual.library import LibraryVisualStrategy, make_stage

_DEFAULT_SEGMENT_SECONDS = 5.0
_DEFAULT_CLIP_SECONDS = 30.0
"""Comfortably longer than ``_DEFAULT_SEGMENT_SECONDS`` so ``_clips``' output
is never filtered out by the duration rule unless a test says otherwise."""


def _clip(clip_id: str, duration_s: float) -> VisualCandidate:
    return VisualCandidate(asset_id=clip_id, duration_s=duration_s)


def _clips(n: int) -> list[VisualCandidate]:
    return [_clip(f"clip-{i}", _DEFAULT_CLIP_SECONDS) for i in range(n)]


def _select(
    *,
    n_segments: int,
    clips: list[VisualCandidate],
    seed: int,
    segment_seconds: float = _DEFAULT_SEGMENT_SECONDS,
) -> list[dict[str, object]]:
    """Run ``LibraryVisualStrategy.plan`` and reshape its result into the
    same ``{"clip_id", "in_point_s", "duration_s"}`` dict shape
    ``segments.json`` uses - the brief's Step 1 assertions read
    ``s["clip_id"]``, so this is the natural boundary for the helper rather
    than exposing ``VisualPlacement`` objects to the test bodies."""
    placements = LibraryVisualStrategy().plan([segment_seconds] * n_segments, clips, seed=seed)
    return [
        {"clip_id": p.asset_id, "in_point_s": p.in_point_s, "duration_s": p.duration_s}
        for p in placements
    ]


# --- Step 1 of this task's brief, verbatim -----------------------------------


def test_no_clip_repeats_until_the_library_is_exhausted() -> None:
    """Repetition within one video is the most visible quality failure."""
    segments = _select(n_segments=4, clips=_clips(6), seed=1)
    assert len({s["clip_id"] for s in segments}) == 4


def test_selection_wraps_when_there_are_more_segments_than_clips() -> None:
    segments = _select(n_segments=5, clips=_clips(2), seed=1)
    assert len(segments) == 5


def test_the_same_seed_selects_the_same_clips() -> None:
    assert _select(n_segments=4, clips=_clips(6), seed=7) == _select(
        n_segments=4, clips=_clips(6), seed=7
    )


def test_a_clip_shorter_than_its_segment_is_never_chosen_for_it() -> None:
    """A short clip would leave the tail of the segment black."""
    segments = _select(
        n_segments=1,
        clips=[_clip("short", 1.0), _clip("long", 30.0)],
        seed=1,
        segment_seconds=5.0,
    )
    assert segments[0]["clip_id"] == "long"


def test_an_empty_library_is_a_fatal_provider_error() -> None:
    with pytest.raises(ProviderError) as exc:
        _select(n_segments=1, clips=[], seed=1)
    assert exc.value.kind is ErrorKind.FATAL


# --- Coverage beyond the brief's Step 1 --------------------------------------


def test_a_different_seed_can_select_different_clips() -> None:
    """The mirror of ``test_the_same_seed_selects_the_same_clips`` - without
    this, a strategy that ignored ``seed`` entirely (e.g. always returning
    ``candidates`` in their given order) would still pass every test above."""
    a = _select(n_segments=6, clips=_clips(6), seed=1)
    b = _select(n_segments=6, clips=_clips(6), seed=2)
    assert [s["clip_id"] for s in a] != [s["clip_id"] for s in b]


def test_no_segments_needs_no_clips() -> None:
    """Zero segments is legal input (a silent, zero-duration timeline is
    already legal per ``core.pipeline.timeline``) - it must not raise even
    against an empty library, since nothing is actually being asked for."""
    assert LibraryVisualStrategy().plan([], [], seed=1) == ()


def test_an_exact_length_match_gets_an_in_point_of_zero() -> None:
    """Ambiguity resolution #5, pinned positively: a clip exactly as long as
    its segment has nowhere to offset from."""
    segments = _select(n_segments=1, clips=[_clip("exact", 5.0)], seed=1, segment_seconds=5.0)
    assert segments[0]["in_point_s"] == 0.0


def test_the_in_point_never_runs_the_clip_past_its_own_end() -> None:
    """``in_point_s + segment_duration`` must never exceed the clip's own
    ``duration_s`` - otherwise a compose stage reading past the clip's real
    length would either fail or freeze on the last frame, neither of which
    is the seeded, reproducible offset the brief asks for."""
    clip = _clip("only", 12.0)
    for seed in range(20):
        segments = _select(n_segments=1, clips=[clip], seed=seed, segment_seconds=5.0)
        in_point_s = segments[0]["in_point_s"]
        assert isinstance(in_point_s, float)
        assert 0.0 <= in_point_s <= clip.duration_s - 5.0


def test_too_short_error_names_the_segment_duration_and_the_longest_clip() -> None:
    """Ambiguity resolution #6: "that is the difference between a user
    knowing to add a longer clip and a user staring at 'selection failed'."
    Guard-pinned on message content, not just ``ErrorKind``."""
    with pytest.raises(ProviderError) as exc:
        _select(
            n_segments=1,
            clips=[_clip("short-a", 2.0), _clip("short-b", 3.0)],
            seed=1,
            segment_seconds=10.0,
        )
    message = str(exc.value)
    assert "10.00" in message, "the segment's own duration must be named"
    assert "3.00" in message, "the longest available clip's duration must be named"


def test_too_short_and_empty_library_errors_are_distinguishable() -> None:
    """Ambiguity resolution #6: "make the message say which" - the two FATAL
    cases must not share one indistinct message, or a user cannot tell "add
    any clip" from "add a longer one" apart."""
    with pytest.raises(ProviderError) as empty_exc:
        _select(n_segments=1, clips=[], seed=1)
    with pytest.raises(ProviderError) as short_exc:
        _select(n_segments=1, clips=[_clip("short", 1.0)], seed=1, segment_seconds=5.0)
    assert str(empty_exc.value) != str(short_exc.value)
    assert "no clips" in str(empty_exc.value) or "empty" in str(empty_exc.value).lower()
    assert "long enough" in str(short_exc.value) or "longest" in str(short_exc.value)


def test_a_clip_too_short_for_only_one_segment_still_serves_the_others() -> None:
    """A clip that cannot fill segment A (too short) may still be perfectly
    eligible for a shorter segment B - the duration filter is per segment,
    not a one-time prune of the whole library.

    Runs across many seeds rather than one fixed seed. This was found to
    matter during this task's guard-pin verification: with the duration
    filter deleted entirely (as a mutation), ``seed=1`` still happened to
    pick "long" first for a 2-candidate ``[short, long]`` pool purely
    because ``random.Random(1).shuffle`` puts "long" at index 0 for that
    exact input - so a single-seed version of this test (and, it turns out,
    the brief's own ``test_a_clip_shorter_than_its_segment_is_never_chosen_for_it``,
    also pinned at ``seed=1``) passes whether or not the filter exists at
    all. See this task's report for the full account. Looping over seeds
    makes the guard fail for the right reason regardless of shuffle luck."""
    for seed in range(20):
        segments = LibraryVisualStrategy().plan(
            [10.0, 2.0],
            [_clip("short", 3.0), _clip("long", 30.0)],
            seed=seed,
        )
        assert segments[0].asset_id == "long", f"only 'long' can fill the 10s segment (seed={seed})"
        # Both clips are eligible for the 2s segment; either is a legal
        # answer - the point of this test is only that selecting it did not
        # raise.
        assert segments[1].asset_id in {"short", "long"}


def test_the_capability_descriptor_declares_a_free_offline_no_gpu_provider() -> None:
    caps = LibraryVisualStrategy.capabilities
    assert caps.provider_id == "library"
    assert caps.cost_model is CostModel.FREE
    assert caps.offline is True, "selection reads only data already in the manifest"
    assert caps.requires_gpu is False
    assert caps.vram_mb is None


def test_library_visual_strategy_conforms_to_the_visualstrategy_protocol() -> None:
    """``VisualStrategy`` is ``@runtime_checkable`` specifically so this is
    cheap to check - without it, ``LibraryVisualStrategy`` could silently
    drift from the Protocol ``SelectBroll`` depends on."""
    assert isinstance(LibraryVisualStrategy(), VisualStrategy)


def test_make_stage_wires_a_libraryvisualstrategy_into_select_broll(tmp_path: Path) -> None:
    """``make_stage`` is the one function allowed to import both
    ``app.stages.select_broll`` and this module's own
    ``LibraryVisualStrategy``; this is the test that its wiring actually
    produces a working stage end to end."""
    import json

    from ytauto.core.models.artifact import ArtifactRef
    from ytauto.core.pipeline.stage import JobContext

    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    try:
        cas = CasStore(root=tmp_path / "cas", conn=conn)

        timeline_bytes = json.dumps(
            {
                "duration_s": 5.0,
                "groups": [],
                "segments": [{"start_s": 0.0, "end_s": 5.0}],
            }
        ).encode("utf-8")
        timeline_digest = cas.stage_file(timeline_bytes, kind="json")

        manifest_bytes = json.dumps(
            [
                {
                    "clip_id": "clip-a",
                    "duration_s": 5.0,
                    "source_width": 1920,
                    "source_height": 1080,
                    "normalised_landscape_digest": "a" * 64,
                    "normalised_vertical_digest": "b" * 64,
                }
            ]
        ).encode("utf-8")
        manifest_digest = cas.stage_file(manifest_bytes, kind="broll_manifest")

        stage = make_stage(cas=cas, settings={})
        assert isinstance(stage, SelectBroll)

        ctx = JobContext(
            job_id="j1",
            project_id="p1",
            settings={"broll_manifest_digest": str(manifest_digest), "seed": 1},
            inputs={
                "plan_timeline": (
                    ArtifactRef(name="timeline.json", kind="json", digest=timeline_digest),
                )
            },
            workdir=tmp_path,
        )
        result = stage.run(ctx, lambda fraction, note: None)

        segments = json.loads(cas.read_bytes(result.artifact("segments.json").digest))
        assert segments == [{"clip_id": "clip-a", "in_point_s": 0.0, "duration_s": 5.0}]
    finally:
        conn.close()


def test_make_stage_ignores_settings_it_has_no_use_for(tmp_path: Path) -> None:
    """``make_stage`` accepts the project's whole settings per the uniform
    factory contract but makes no construction-time decision from them -
    this stage has exactly one provider, and reads its two real settings
    keys through ``ctx.settings`` at run time instead."""
    conn = sqlite3.connect(":memory:")
    try:
        cas = CasStore(root=tmp_path / "cas", conn=conn)
        stage = make_stage(cas=cas, settings={"voice": "en-GB-RyanNeural", "unrelated": 123})
        assert stage.id == "select_broll"
    finally:
        conn.close()
