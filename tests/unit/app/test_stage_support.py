"""The settings projection, and the fingerprint helper built on it.

Criterion 4 of Phase 2a lives here: a stage's fingerprint must depend on the
settings that stage declared and on nothing else. The failure this pins is
silent in both directions - a projection that is too wide re-runs edge-tts
because a caption colour changed, and one that is too narrow serves a cached
narration in the wrong voice.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from ytauto.app.stage_support import project_settings, stage_fingerprint
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult


class _FakeStage:
    """A Stage double that carries only what ``stage_fingerprint`` reads.

    ``run`` is never called: every test here stops at the fingerprint.
    """

    def __init__(self, *, id: str, version: int, settings_keys: tuple[str, ...]) -> None:
        self.id = id
        self.version = version
        self.depends_on: tuple[str, ...] = ()
        self.settings_keys = settings_keys

    def fingerprint(self, ctx: JobContext) -> str:
        return stage_fingerprint(self, ctx, provider_id="edge-tts", provider_version="1")

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        raise NotImplementedError("not exercised: these tests stop at the fingerprint")


def _ctx(
    *,
    settings: Mapping[str, object] | None = None,
    inputs: Mapping[str, tuple[ArtifactRef, ...]] | None = None,
    workdir: Path = Path("/tmp/j1"),
) -> JobContext:
    return JobContext(
        job_id="j1",
        project_id="p1",
        settings={} if settings is None else settings,
        inputs={} if inputs is None else inputs,
        workdir=workdir,
    )


def test_a_stage_fingerprint_ignores_settings_it_did_not_declare() -> None:
    """Changing the caption colour must not re-run edge-tts."""
    stage = _FakeStage(id="synthesize_speech", version=1, settings_keys=("voice",))
    ctx_a = _ctx(settings={"voice": "en-US-GuyNeural", "caption_colour": "#ff0000"})
    ctx_b = _ctx(settings={"voice": "en-US-GuyNeural", "caption_colour": "#00ff00"})

    fp_a = stage_fingerprint(stage, ctx_a, provider_id="edge-tts", provider_version="1")
    fp_b = stage_fingerprint(stage, ctx_b, provider_id="edge-tts", provider_version="1")

    assert fp_a == fp_b, "an undeclared setting must not enter the fingerprint"


def test_a_stage_fingerprint_changes_when_a_declared_setting_changes() -> None:
    """The non-vacuous contrast: without this, returning a constant would pass above."""
    stage = _FakeStage(id="synthesize_speech", version=1, settings_keys=("voice",))

    fp_a = stage_fingerprint(
        stage,
        _ctx(settings={"voice": "en-US-GuyNeural"}),
        provider_id="edge-tts",
        provider_version="1",
    )
    fp_b = stage_fingerprint(
        stage,
        _ctx(settings={"voice": "en-GB-RyanNeural"}),
        provider_id="edge-tts",
        provider_version="1",
    )

    assert fp_a != fp_b, "a declared setting must enter the fingerprint"


def test_the_workdir_never_reaches_a_fingerprint() -> None:
    """``JobContext.workdir`` is job- and machine-specific; a fingerprint that
    moved with it would never hit across jobs at all."""
    stage = _FakeStage(id="s", version=1, settings_keys=("voice",))

    fp_a = stage_fingerprint(
        stage,
        _ctx(workdir=Path("/tmp/a"), settings={"voice": "v"}),
        provider_id="p",
        provider_version="1",
    )
    fp_b = stage_fingerprint(
        stage,
        _ctx(workdir=Path("/tmp/b"), settings={"voice": "v"}),
        provider_id="p",
        provider_version="1",
    )

    assert fp_a == fp_b


def test_projecting_omits_keys_the_settings_do_not_have() -> None:
    """An absent declared key must be omitted, not defaulted to None -
    a None would enter the hash and differ from the key being absent."""
    assert project_settings({"voice": "v"}, ("voice", "rate")) == {"voice": "v"}


def test_projecting_an_empty_key_tuple_yields_an_empty_mapping() -> None:
    """A stage that declares no settings must fingerprint identically
    regardless of what the project settings contain."""
    assert project_settings({"voice": "v", "seed": 3}, ()) == {}


def test_an_absent_declared_key_is_not_the_same_as_a_null_one() -> None:
    """The pair the docstring above only claims in prose.

    ``{"rate": None}`` and ``{}`` are different JSON documents, so a
    projection that defaulted a missing key to None would give a stage a
    different fingerprint depending on whether an unrelated key had ever been
    written to the project - a silent cache miss on every existing artifact.
    """
    stage = _FakeStage(id="s", version=1, settings_keys=("voice", "rate"))

    absent = stage_fingerprint(
        stage, _ctx(settings={"voice": "v"}), provider_id="p", provider_version="1"
    )
    null = stage_fingerprint(
        stage, _ctx(settings={"voice": "v", "rate": None}), provider_id="p", provider_version="1"
    )

    assert absent != null


def test_the_fingerprint_is_stable_across_settings_insertion_order() -> None:
    """Two processes building the same settings dict in different orders must
    agree, or the cache silently stops working across process boundaries."""
    stage = _FakeStage(id="s", version=1, settings_keys=("voice", "rate"))

    forward = stage_fingerprint(
        stage, _ctx(settings={"voice": "v", "rate": 2}), provider_id="p", provider_version="1"
    )
    reverse = stage_fingerprint(
        stage, _ctx(settings={"rate": 2, "voice": "v"}), provider_id="p", provider_version="1"
    )

    assert forward == reverse
