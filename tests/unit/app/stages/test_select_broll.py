"""``SelectBroll``: the fifth stage of the ``story_video`` pipeline.

Typed against ``core.ports.providers.VisualStrategy``, never against a
concrete provider class - ``ytauto.app`` may not import ``ytauto.providers``
(an import-linter ``forbidden`` contract). Every test here exercises
``SelectBroll`` with a fake ``VisualStrategy`` injected, both because that
keeps this suite off the real ``LibraryVisualStrategy`` (whose own selection
algorithm is ``tests/unit/providers/test_library_visual.py``'s job to cover)
and because this project's history is that every stage gets at least one
test driving its own ``run()`` rather than only the provider in isolation -
see ``test_transcribe.py``'s module docstring for the precedent this follows.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest

from ytauto.app.stages.select_broll import SelectBroll
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash
from ytauto.core.models.visual import VisualCandidate, VisualPlacement
from ytauto.core.pipeline.stage import JobContext
from ytauto.core.ports.capability import CapabilityDescriptor, CostModel, LatencyClass
from ytauto.infra.cas.store import CasStore

# db_conn is defined in tests/unit/conftest.py.

_Placements = tuple[VisualPlacement, ...]


class _FakeVisualStrategy:
    """A ``VisualStrategy`` double that never touches ``LibraryVisualStrategy``.

    Its ``capabilities`` deliberately differ from ``LibraryVisualStrategy``'s
    so that a stage which (wrongly) read provider identity off the injected
    object would fingerprint differently once this is substituted in - the
    exact failure mode
    ``test_the_fingerprint_provider_identity_is_literal_not_injected`` guards
    against.
    """

    capabilities = CapabilityDescriptor(
        provider_id="fake-visual-strategy",
        version="99",
        cost_model=CostModel.FREE,
        latency_class=LatencyClass.INSTANT,
        offline=True,
        requires_gpu=False,
        vram_mb=None,
        quality_tier=1,
        languages=frozenset({"und"}),
    )

    def __init__(self, result: _Placements) -> None:
        self._result = result
        self.calls: list[tuple[Sequence[float], Sequence[VisualCandidate], int]] = []

    def plan(
        self,
        segment_durations: Sequence[float],
        candidates: Sequence[VisualCandidate],
        *,
        seed: int,
    ) -> _Placements:
        self.calls.append((segment_durations, candidates, seed))
        return self._result


class _RaisingVisualStrategy:
    """A ``VisualStrategy`` double whose ``plan`` always fails."""

    capabilities = _FakeVisualStrategy.capabilities

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def plan(
        self,
        segment_durations: Sequence[float],
        candidates: Sequence[VisualCandidate],
        *,
        seed: int,
    ) -> _Placements:
        raise self._exc


class _OtherFakeVisualStrategy:
    """A second, distinct ``VisualStrategy`` double - a genuinely different
    class from ``_FakeVisualStrategy``, not just a second instance of it,
    with its own ``provider_id``/``version`` on ``capabilities``. Mirrors
    ``test_transcribe.py``'s ``_OtherFakeTranscriber``: two instances of one
    class share a class-level ``capabilities`` object, so swapping one
    instance for another would prove nothing about whether ``fingerprint``
    reads identity off the injected object or off a literal."""

    capabilities = CapabilityDescriptor(
        provider_id="other-fake-visual-strategy",
        version="1",
        cost_model=CostModel.FREE,
        latency_class=LatencyClass.INSTANT,
        offline=True,
        requires_gpu=False,
        vram_mb=None,
        quality_tier=1,
        languages=frozenset({"und"}),
    )

    def __init__(self, result: _Placements) -> None:
        self._result = result

    def plan(
        self,
        segment_durations: Sequence[float],
        candidates: Sequence[VisualCandidate],
        *,
        seed: int,
    ) -> _Placements:
        return self._result


_ARBITRARY_DIGEST = ContentHash("a" * 64)
_OTHER_DIGEST = ContentHash("b" * 64)

_FULL_SETTINGS: dict[str, object] = {
    "broll_manifest_digest": "c" * 64,
    "seed": 1,
}


@pytest.fixture()
def cas(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    return CasStore(root=tmp_path / "cas", conn=db_conn)


def _timeline_bytes(segments: list[tuple[float, float]]) -> bytes:
    return json.dumps(
        {
            "duration_s": segments[-1][1] if segments else 0.0,
            "groups": [],
            "segments": [{"start_s": s, "end_s": e} for s, e in segments],
        }
    ).encode("utf-8")


def _manifest_bytes(entries: list[tuple[str, float]]) -> bytes:
    return json.dumps(
        [
            {
                "clip_id": clip_id,
                "duration_s": duration_s,
                "source_width": 1920,
                "source_height": 1080,
                "normalised_landscape_digest": "d" * 64,
                "normalised_vertical_digest": "e" * 64,
            }
            for clip_id, duration_s in entries
        ]
    ).encode("utf-8")


def _ctx(
    *,
    settings: Mapping[str, object] | None = None,
    timeline_digest: ContentHash = _ARBITRARY_DIGEST,
    workdir: Path = Path("/tmp/j1"),
) -> JobContext:
    return JobContext(
        job_id="j1",
        project_id="p1",
        settings=_FULL_SETTINGS if settings is None else settings,
        inputs={
            "plan_timeline": (
                ArtifactRef(name="timeline.json", kind="json", digest=timeline_digest),
            )
        },
        workdir=workdir,
    )


def test_stage_identity_and_declared_settings(cas: CasStore) -> None:
    stage = SelectBroll(cas=cas, visual_strategy=_FakeVisualStrategy(()))
    assert stage.id == "select_broll"
    assert stage.version == 1
    assert stage.depends_on == ("plan_timeline",)
    assert stage.settings_keys == ("broll_manifest_digest", "seed")


def test_the_fingerprint_provider_identity_is_literal_not_injected(cas: CasStore) -> None:
    """This task's brief: literal provider-identity constants, as Tasks 5 and
    6 do - not values read off the injected provider. Two stages differing
    only in which ``VisualStrategy`` they were given must fingerprint
    identically, or a factory that later picks a provider from settings
    could make the dispatcher and a worker disagree about one cached stage."""
    ctx = _ctx()
    stage_a = SelectBroll(cas=cas, visual_strategy=_FakeVisualStrategy(()))
    stage_b = SelectBroll(cas=cas, visual_strategy=_OtherFakeVisualStrategy(()))
    assert stage_a.fingerprint(ctx) == stage_b.fingerprint(ctx)


def test_the_fingerprint_follows_the_upstream_timeline_digest(cas: CasStore) -> None:
    """A re-planned timeline (new ``timeline.json``, e.g. after a caption
    setting changes upstream) must invalidate this stage's cache - otherwise
    it would silently serve B-roll selected for a different edit."""
    stage = SelectBroll(cas=cas, visual_strategy=_FakeVisualStrategy(()))
    fp_a = stage.fingerprint(_ctx(timeline_digest=_ARBITRARY_DIGEST))
    fp_b = stage.fingerprint(_ctx(timeline_digest=_OTHER_DIGEST))
    assert fp_a != fp_b


def test_the_fingerprint_follows_the_manifest_digest(cas: CasStore) -> None:
    """Adding a clip to the library changes ``broll_manifest_digest`` and
    must invalidate selection - the whole reason the manifest digest is in
    ``settings_keys`` at all."""
    stage = SelectBroll(cas=cas, visual_strategy=_FakeVisualStrategy(()))
    fp_a = stage.fingerprint(_ctx(settings={"broll_manifest_digest": "c" * 64, "seed": 1}))
    fp_b = stage.fingerprint(_ctx(settings={"broll_manifest_digest": "d" * 64, "seed": 1}))
    assert fp_a != fp_b


def test_the_fingerprint_follows_the_seed(cas: CasStore) -> None:
    stage = SelectBroll(cas=cas, visual_strategy=_FakeVisualStrategy(()))
    fp_a = stage.fingerprint(_ctx(settings={"broll_manifest_digest": "c" * 64, "seed": 1}))
    fp_b = stage.fingerprint(_ctx(settings={"broll_manifest_digest": "c" * 64, "seed": 2}))
    assert fp_a != fp_b


def test_the_fingerprint_ignores_unrelated_settings(cas: CasStore) -> None:
    """``settings_keys`` is exactly ``("broll_manifest_digest", "seed")`` -
    an unrelated setting (e.g. a caption colour) changing must not
    invalidate this stage's cache."""
    stage = SelectBroll(cas=cas, visual_strategy=_FakeVisualStrategy(()))
    fp_a = stage.fingerprint(
        _ctx(settings={"broll_manifest_digest": "c" * 64, "seed": 1, "caption_colour": "white"})
    )
    fp_b = stage.fingerprint(
        _ctx(settings={"broll_manifest_digest": "c" * 64, "seed": 1, "caption_colour": "yellow"})
    )
    assert fp_a == fp_b


def test_run_calls_plan_with_segment_durations_derived_from_the_timeline(cas: CasStore) -> None:
    """``timeline.json``'s segments carry ``start_s``/``end_s`` - the
    injected strategy must see each segment's *duration*
    (``end_s - start_s``), not the raw boundaries."""
    timeline_digest = cas.stage_file(_timeline_bytes([(0.0, 5.0), (5.0, 9.0)]), kind="json")
    manifest_digest = cas.stage_file(_manifest_bytes([("clip-a", 30.0)]), kind="broll_manifest")
    fake = _FakeVisualStrategy(
        (
            VisualPlacement(asset_id="clip-a", in_point_s=0.0, duration_s=5.0),
            VisualPlacement(asset_id="clip-a", in_point_s=1.0, duration_s=4.0),
        )
    )
    stage = SelectBroll(cas=cas, visual_strategy=fake)
    ctx = _ctx(
        settings={"broll_manifest_digest": str(manifest_digest), "seed": 3},
        timeline_digest=timeline_digest,
    )

    stage.run(ctx, lambda fraction, note: None)

    assert len(fake.calls) == 1
    segment_durations, candidates, seed = fake.calls[0]
    assert list(segment_durations) == [5.0, 4.0]
    assert seed == 3


def test_run_reads_clip_id_and_duration_from_the_manifest(cas: CasStore) -> None:
    """Only ``clip_id``/``duration_s`` are read from the manifest here - the
    two digest columns are Tasks 11/12's concern, resolved per canvas, never
    this stage's."""
    timeline_digest = cas.stage_file(_timeline_bytes([(0.0, 5.0)]), kind="json")
    manifest_digest = cas.stage_file(
        _manifest_bytes([("clip-a", 12.0), ("clip-b", 30.0)]), kind="broll_manifest"
    )
    fake = _FakeVisualStrategy(
        (VisualPlacement(asset_id="clip-a", in_point_s=0.0, duration_s=5.0),)
    )
    stage = SelectBroll(cas=cas, visual_strategy=fake)
    ctx = _ctx(
        settings={"broll_manifest_digest": str(manifest_digest), "seed": 1},
        timeline_digest=timeline_digest,
    )

    stage.run(ctx, lambda fraction, note: None)

    _, candidates, _ = fake.calls[0]
    assert set(candidates) == {
        VisualCandidate(asset_id="clip-a", duration_s=12.0),
        VisualCandidate(asset_id="clip-b", duration_s=30.0),
    }


def test_run_emits_segments_json_naming_clip_id_never_a_digest(cas: CasStore) -> None:
    """The load-bearing shape from this task's brief:
    ``{"clip_id", "in_point_s", "duration_s"}`` - never a digest, so one
    selection serves both compose stages' own canvas resolution."""
    timeline_digest = cas.stage_file(_timeline_bytes([(0.0, 5.0), (5.0, 9.0)]), kind="json")
    manifest_digest = cas.stage_file(_manifest_bytes([("clip-a", 30.0)]), kind="broll_manifest")
    fake = _FakeVisualStrategy(
        (
            VisualPlacement(asset_id="clip-a", in_point_s=2.5, duration_s=5.0),
            VisualPlacement(asset_id="clip-a", in_point_s=0.0, duration_s=4.0),
        )
    )
    stage = SelectBroll(cas=cas, visual_strategy=fake)
    ctx = _ctx(
        settings={"broll_manifest_digest": str(manifest_digest), "seed": 1},
        timeline_digest=timeline_digest,
    )

    result = stage.run(ctx, lambda fraction, note: None)

    segments_ref = result.artifact("segments.json")
    assert segments_ref.kind == "json"
    assert json.loads(cas.read_bytes(segments_ref.digest)) == [
        {"clip_id": "clip-a", "in_point_s": 2.5, "duration_s": 5.0},
        {"clip_id": "clip-a", "in_point_s": 0.0, "duration_s": 4.0},
    ]


def test_run_propagates_a_provider_error_from_the_injected_strategy(cas: CasStore) -> None:
    """``run`` does not swallow the strategy's error - it is ``run_stage``
    (the worker's caller), not the stage, that translates it into a
    worker-protocol message."""
    timeline_digest = cas.stage_file(_timeline_bytes([(0.0, 5.0)]), kind="json")
    manifest_digest = cas.stage_file(_manifest_bytes([]), kind="broll_manifest")
    stage = SelectBroll(
        cas=cas,
        visual_strategy=_RaisingVisualStrategy(
            ProviderError("boom", provider_id="fake-visual-strategy", kind=ErrorKind.FATAL)
        ),
    )
    ctx = _ctx(
        settings={"broll_manifest_digest": str(manifest_digest), "seed": 1},
        timeline_digest=timeline_digest,
    )

    with pytest.raises(ProviderError) as exc:
        stage.run(ctx, lambda fraction, note: None)
    assert exc.value.kind is ErrorKind.FATAL


def test_run_writes_exactly_what_the_strategy_returned_not_a_recomputation(cas: CasStore) -> None:
    """Mirrors ``test_transcribe.py``'s identically-named test: ``run()``
    calling ``self._visual_strategy.plan(...)`` for its side effect and then
    silently recomputing ``segments.json`` from the timeline/manifest itself,
    instead of trusting what came back, would pass a naive "was the fake
    invoked" check while producing the wrong output. The manifest here names
    only clips the fake's return value does *not* reference, so a stage that
    recomputed inline would emit one of those clip ids instead of the fake's
    sentinel - deliberately unrelated, the same reasoning
    ``test_transcribe.py`` documents for why its own fixture cannot be
    realistic-and-therefore-coincidentally-correct."""
    timeline_digest = cas.stage_file(_timeline_bytes([(0.0, 5.0)]), kind="json")
    manifest_digest = cas.stage_file(
        _manifest_bytes([("unrelated-a", 30.0), ("unrelated-b", 30.0)]), kind="broll_manifest"
    )
    fake = _FakeVisualStrategy(
        (
            VisualPlacement(
                asset_id="only-the-fake-could-produce-this", in_point_s=0.0, duration_s=5.0
            ),
        )
    )
    stage = SelectBroll(cas=cas, visual_strategy=fake)
    ctx = _ctx(
        settings={"broll_manifest_digest": str(manifest_digest), "seed": 1},
        timeline_digest=timeline_digest,
    )

    result = stage.run(ctx, lambda fraction, note: None)

    assert json.loads(cas.read_bytes(result.artifact("segments.json").digest)) == [
        {"clip_id": "only-the-fake-could-produce-this", "in_point_s": 0.0, "duration_s": 5.0}
    ]
