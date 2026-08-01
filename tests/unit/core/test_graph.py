import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.pipeline.graph import Pipeline
from ytauto.core.pipeline.stage import JobContext, StageResult


class FakeStage:
    """Minimal Stage implementation for graph tests."""

    def __init__(self, stage_id: str, depends_on: tuple[str, ...] = ()) -> None:
        self.id = stage_id
        self.version = 1
        self.depends_on = depends_on

    def fingerprint(self, ctx: JobContext) -> str:
        return "f" * 64

    def run(self, ctx: JobContext, emit: object) -> StageResult:
        return StageResult(artifacts=())


def _linear() -> Pipeline:
    return Pipeline(
        id="shorts",
        stages=(
            FakeStage("ingest"),
            FakeStage("rewrite", ("ingest",)),
            FakeStage("tts", ("rewrite",)),
        ),
    )


def test_topological_order_respects_dependencies() -> None:
    assert [s.id for s in _linear().topological_order()] == ["ingest", "rewrite", "tts"]


def test_topological_order_is_deterministic_regardless_of_declaration_order() -> None:
    """Two runs must plan identically; a varying order could vary fingerprints."""
    shuffled = Pipeline(
        id="shorts",
        stages=(
            FakeStage("tts", ("rewrite",)),
            FakeStage("ingest"),
            FakeStage("rewrite", ("ingest",)),
        ),
    )
    assert [s.id for s in shuffled.topological_order()] == ["ingest", "rewrite", "tts"]


def test_independent_stages_are_ordered_by_id() -> None:
    pipeline = Pipeline(
        id="p",
        stages=(FakeStage("zebra"), FakeStage("apple"), FakeStage("mango")),
    )
    assert [s.id for s in pipeline.topological_order()] == ["apple", "mango", "zebra"]


def test_diamond_dependencies_resolve() -> None:
    pipeline = Pipeline(
        id="p",
        stages=(
            FakeStage("root"),
            FakeStage("left", ("root",)),
            FakeStage("right", ("root",)),
            FakeStage("join", ("left", "right")),
        ),
    )
    order = [s.id for s in pipeline.topological_order()]
    assert order[0] == "root"
    assert order[-1] == "join"
    assert set(order[1:3]) == {"left", "right"}


def test_a_cycle_is_rejected() -> None:
    with pytest.raises(ValidationError, match="cycle"):
        Pipeline(id="p", stages=(FakeStage("a", ("b",)), FakeStage("b", ("a",))))


def test_a_self_dependency_is_rejected() -> None:
    with pytest.raises(ValidationError, match="cycle"):
        Pipeline(id="p", stages=(FakeStage("a", ("a",)),))


def test_an_unknown_dependency_is_rejected() -> None:
    with pytest.raises(ValidationError, match="ghost"):
        Pipeline(id="p", stages=(FakeStage("a", ("ghost",)),))


def test_duplicate_stage_ids_are_rejected() -> None:
    with pytest.raises(ValidationError, match="duplicate"):
        Pipeline(id="p", stages=(FakeStage("a"), FakeStage("a")))


def test_an_empty_pipeline_is_rejected() -> None:
    with pytest.raises(ValidationError, match="empty"):
        Pipeline(id="p", stages=())


def test_stage_by_id_returns_the_stage() -> None:
    assert _linear().stage_by_id("rewrite").id == "rewrite"


def test_stage_by_id_rejects_an_unknown_id() -> None:
    with pytest.raises(ValidationError, match="nope"):
        _linear().stage_by_id("nope")


def test_downstream_is_transitive() -> None:
    """Editing the script must invalidate tts too, not just rewrite."""
    assert _linear().downstream_of("ingest") == {"rewrite", "tts"}


def test_downstream_of_a_leaf_is_empty() -> None:
    assert _linear().downstream_of("tts") == frozenset()


def test_downstream_excludes_the_stage_itself() -> None:
    assert "rewrite" not in _linear().downstream_of("rewrite")


def test_downstream_across_a_diamond_reaches_the_join() -> None:
    pipeline = Pipeline(
        id="p",
        stages=(
            FakeStage("root"),
            FakeStage("left", ("root",)),
            FakeStage("right", ("root",)),
            FakeStage("join", ("left", "right")),
        ),
    )
    assert pipeline.downstream_of("left") == {"join"}
    assert pipeline.downstream_of("root") == {"left", "right", "join"}


def test_downstream_rejects_an_unknown_stage() -> None:
    with pytest.raises(ValidationError, match="nope"):
        _linear().downstream_of("nope")
