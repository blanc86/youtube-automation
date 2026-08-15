from pathlib import Path

import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash, hash_bytes
from ytauto.core.pipeline.stage import JobContext, Stage, StageResult


def _ref(name: str) -> ArtifactRef:
    return ArtifactRef(name=name, kind="blob", digest=hash_bytes(name.encode()))


@pytest.fixture()
def fake_digest() -> ContentHash:
    return hash_bytes(b"fake")


def _ctx(**overrides: object) -> JobContext:
    base: dict[str, object] = {
        "job_id": "j1",
        "project_id": "p1",
        "settings": {"voice": "en-GB"},
        "inputs": {"ingest": (_ref("story"),)},
        "workdir": Path("/tmp/j1"),
    }
    base.update(overrides)
    return JobContext(**base)  # type: ignore[arg-type]


def test_context_exposes_a_named_input() -> None:
    assert _ctx().input("ingest", "story").name == "story"


def test_missing_input_stage_raises() -> None:
    with pytest.raises(ValidationError, match="rewrite"):
        _ctx().input("rewrite", "script")


def test_missing_input_name_raises() -> None:
    with pytest.raises(ValidationError, match="script"):
        _ctx().input("ingest", "script")


def test_context_is_frozen() -> None:
    with pytest.raises(AttributeError):
        _ctx().job_id = "other"  # type: ignore[misc]


def test_result_exposes_a_named_artifact() -> None:
    result = StageResult(artifacts=(_ref("narration"), _ref("timings")))
    assert result.artifact("timings").name == "timings"


def test_result_rejects_duplicate_artifact_names() -> None:
    with pytest.raises(ValidationError, match="duplicate"):
        StageResult(artifacts=(_ref("narration"), _ref("narration")))


def test_artifacts_are_sorted_by_name(fake_digest: ContentHash) -> None:
    """Declaration order must equal name order, because ArtifactStore.lookup
    returns name order and a stage's fresh and cached paths must agree."""
    result = StageResult(
        artifacts=(
            ArtifactRef(name="timings", kind="blob", digest=fake_digest),
            ArtifactRef(name="narration", kind="blob", digest=fake_digest),
        ),
        meta={},
    )
    assert [a.name for a in result.artifacts] == ["narration", "timings"]


def test_a_stage_declaring_reverse_order_still_round_trips(fake_digest: ContentHash) -> None:
    """The property that matters: whatever order the stage declared, the tuple
    matches what a later lookup() will hand back."""
    declared = StageResult(
        artifacts=(
            ArtifactRef(name="zulu", kind="blob", digest=fake_digest),
            ArtifactRef(name="alpha", kind="blob", digest=fake_digest),
        ),
        meta={},
    )
    assert [a.name for a in declared.artifacts] == sorted(a.name for a in declared.artifacts)


def test_missing_result_artifact_raises() -> None:
    with pytest.raises(ValidationError, match="absent"):
        StageResult(artifacts=(_ref("narration"),)).artifact("absent")


def test_result_defaults_to_empty_meta() -> None:
    assert StageResult(artifacts=()).meta == {}


def test_a_conforming_class_satisfies_the_protocol() -> None:
    class Echo:
        id = "echo"
        version = 1
        depends_on: tuple[str, ...] = ()
        settings_keys: tuple[str, ...] = ()

        def fingerprint(self, ctx: JobContext) -> str:
            return "f" * 64

        def run(self, ctx: JobContext, emit: object) -> StageResult:
            return StageResult(artifacts=())

    assert isinstance(Echo(), Stage)


def test_a_class_missing_run_does_not_satisfy_the_protocol() -> None:
    class Partial:
        id = "partial"
        version = 1
        depends_on: tuple[str, ...] = ()
        settings_keys: tuple[str, ...] = ()

        def fingerprint(self, ctx: JobContext) -> str:
            return "f" * 64

    assert not isinstance(Partial(), Stage)


def test_a_class_that_declares_no_settings_keys_does_not_satisfy_the_protocol() -> None:
    """``settings_keys`` is required, not optional-with-a-default.

    A stage that simply omitted it would fall through to
    ``AttributeError`` deep inside ``stage_fingerprint`` at run time - in a
    worker subprocess, mid-job. A stage that reads no settings says so
    explicitly with ``()``.
    """

    class Undeclared:
        id = "undeclared"
        version = 1
        depends_on: tuple[str, ...] = ()

        def fingerprint(self, ctx: JobContext) -> str:
            return "f" * 64

        def run(self, ctx: JobContext, emit: object) -> StageResult:
            return StageResult(artifacts=())

    assert not isinstance(Undeclared(), Stage)
