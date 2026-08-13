import sqlite3
from collections.abc import Sequence
from pathlib import Path

import pytest

from ytauto.app.scheduler.runner import RunnerContext, build_spec, gather_inputs, run_stage
from ytauto.app.scheduler.worker_protocol import Error, Result
from ytauto.core.errors import ErrorKind, ProviderError, ValidationError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import hash_bytes
from ytauto.core.pipeline.fingerprint import compute_fingerprint
from ytauto.core.pipeline.graph import Pipeline
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.cas.store import CasStore

# db_conn is defined in tests/unit/conftest.py.


@pytest.fixture()
def store(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    """A CasStore sharing the migrated connection from ``db_conn``."""
    return CasStore(root=tmp_path / "cas", conn=db_conn)


@pytest.fixture()
def artifacts(store: CasStore, db_conn: sqlite3.Connection) -> ArtifactStore:
    return ArtifactStore(store, db_conn)


class _FakeStage:
    """A minimal double satisfying ``core.pipeline.stage.Stage``'s shape.

    ``calls`` increments on every ``run()`` invocation so later tasks (the
    dispatcher's crash-resume test) can assert a stage did NOT re-run without
    depending on timing.

    Its produced artifacts are staged for real, through ``cas.stage_file``,
    when ``cas`` is supplied - mirroring how a real Stage implementation
    would write its own output through its own injected CasStore. When
    ``cas`` is omitted the digest is still computed (via the same
    ``hash_bytes`` CasStore itself uses) but nothing is written to disk -
    which is exactly the "claimed an artifact it never staged" scenario
    ``run_stage``'s verification step exists to catch.
    """

    def __init__(
        self,
        stage_id: str,
        depends_on: tuple[str, ...] = (),
        *,
        produces: Sequence[tuple[str, bytes]] = (),
        raises: Exception | None = None,
        cas: CasStore | None = None,
    ) -> None:
        self.id = stage_id
        self.version = 1
        self.depends_on = depends_on
        self._produces = tuple(produces)
        self._raises = raises
        self._cas = cas
        self.calls = 0

    def fingerprint(self, ctx: JobContext) -> str:
        return "f" * 64

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        artifacts = tuple(
            ArtifactRef(
                name=name,
                kind="blob",
                digest=(
                    self._cas.stage_file(data, kind="blob")
                    if self._cas is not None
                    else hash_bytes(data)
                ),
            )
            for name, data in self._produces
        )
        return StageResult(artifacts=artifacts)


def _fake_stage(
    stage_id: str,
    *,
    produces: Sequence[tuple[str, bytes]] = (),
    raises: Exception | None = None,
    cas: CasStore | None = None,
) -> _FakeStage:
    return _FakeStage(stage_id, produces=produces, raises=raises, cas=cas)


def _stage(stage_id: str) -> _FakeStage:
    """A stage identity with no behaviour, for build_spec's tests - it only
    ever reads ``.id`` and ``.version``."""
    return _FakeStage(stage_id)


def _chain() -> Pipeline:
    """fetch -> tts -> render."""
    return Pipeline(
        id="p",
        stages=(
            _FakeStage("fetch"),
            _FakeStage("tts", ("fetch",)),
            _FakeStage("render", ("tts",)),
        ),
    )


def _ref(name: str) -> ArtifactRef:
    return ArtifactRef(name=name, kind="blob", digest=hash_bytes(name.encode()))


def _put(store: CasStore, name: str, data: bytes) -> ArtifactRef:
    return ArtifactRef(name=name, kind="blob", digest=store.put_bytes(data, kind="blob"))


def _ctx(**overrides: object) -> RunnerContext:
    job_id = str(overrides.pop("job_id", "j1"))
    correlation_id = str(overrides.pop("correlation_id", "c1"))
    job_overrides: dict[str, object] = {
        "job_id": job_id,
        "project_id": "p1",
        "settings": {},
        "inputs": {},
        "workdir": Path("/tmp/j1"),
    }
    job_overrides.update(overrides)
    return RunnerContext(
        job=JobContext(**job_overrides),  # type: ignore[arg-type]
        job_id=job_id,
        correlation_id=correlation_id,
    )


def test_a_RunnerContext_whose_two_job_ids_disagree_is_refused() -> None:
    """``job_id`` is duplicated on purpose (a protocol message needs it
    without reaching through the domain object), and two sources of truth for
    one value is a footgun - so the two are checked to agree rather than
    trusted to. Every protocol message a worker sends is addressed with
    ``RunnerContext.job_id``, so a mismatch would file a stage's result and
    its errors under a job that never ran it."""
    job = JobContext(job_id="j1", project_id="p1", settings={}, inputs={}, workdir=Path("/tmp/j1"))

    with pytest.raises(ValidationError, match="does not match"):
        RunnerContext(job=job, job_id="j2", correlation_id="c1")


def test_a_RunnerContext_whose_job_ids_agree_is_accepted() -> None:
    """The contrast that keeps the guard above from passing vacuously."""
    job = JobContext(job_id="j1", project_id="p1", settings={}, inputs={}, workdir=Path("/tmp/j1"))

    ctx = RunnerContext(job=job, job_id="j1", correlation_id="c1")

    assert ctx.job_id == ctx.job.job_id == "j1"


def test_gather_inputs_collects_every_upstream_stage(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    """upstream_of is transitive, so a stage sees its grandparents' artifacts
    too, not only its direct parents'."""
    pipeline = _chain()  # fetch -> tts -> render
    fetch_ref = _put(store, "story", b"text")
    tts_ref = _put(store, "narration", b"audio")
    artifacts.record("a" * 64, "fetch", [fetch_ref])
    artifacts.record("b" * 64, "tts", [tts_ref])

    inputs = gather_inputs(pipeline, "render", {"fetch": "a" * 64, "tts": "b" * 64}, artifacts)

    assert set(inputs) == {"fetch", "tts"}
    assert [a.name for a in inputs["fetch"]] == ["story"]


def test_gather_inputs_skips_a_stage_with_no_recorded_fingerprint(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    """On a resume, an upstream stage that has not run yet contributes nothing
    rather than raising - the runner is asked for what exists."""
    pipeline = _chain()
    assert gather_inputs(pipeline, "render", {}, artifacts) == {}


def test_build_spec_orders_inputs_by_stage_then_artifact_name() -> None:
    """The flattening rule, stated once. Without it two stage authors pick two
    orders and the same stage fingerprints differently in each."""
    inputs = {
        "tts": (_ref("timings"), _ref("narration")),
        "fetch": (_ref("story"),),
    }
    spec = build_spec(_stage("render"), "ffmpeg", "7.1", inputs, {})
    assert [name for name, _ in spec.input_digests] == ["story", "narration", "timings"]


def test_build_spec_is_stable_across_dict_iteration_order() -> None:
    """Fingerprints must not depend on mapping order - a plain dict would make
    the same inputs fingerprint differently between processes."""
    a = build_spec(_stage("r"), "p", "1", {"x": (_ref("n"),), "y": (_ref("m"),)}, {})
    b = build_spec(_stage("r"), "p", "1", {"y": (_ref("m"),), "x": (_ref("n"),)}, {})
    assert compute_fingerprint(a) == compute_fingerprint(b)


def test_run_stage_returns_a_result_carrying_the_stage_s_artifacts(store: CasStore) -> None:
    stage = _fake_stage("tts", produces=[("narration", b"audio")], cas=store)
    message = run_stage(stage, _ctx(job_id="j1", correlation_id="c1"), store)

    assert isinstance(message, Result)
    assert [a.name for a in message.artifacts] == ["narration"]
    assert message.job_id == "j1" and message.stage_id == "tts"


def test_a_stage_raising_ProviderError_becomes_an_Error_message_with_its_kind(
    store: CasStore,
) -> None:
    """The dispatcher maps kind to FATAL vs RETRYABLE; losing it here would make
    every failure look the same and a rate limit would burn the job's attempts."""
    stage = _fake_stage(
        "tts",
        raises=ProviderError(
            "429", provider_id="elevenlabs", kind=ErrorKind.RATE_LIMITED, retry_after_s=30.0
        ),
    )
    message = run_stage(stage, _ctx(job_id="j1", correlation_id="c1"), store)

    assert isinstance(message, Error)
    assert message.kind is ErrorKind.RATE_LIMITED
    assert message.retry_after_s == 30.0


def test_a_stage_raising_an_unexpected_exception_becomes_a_FATAL_Error(store: CasStore) -> None:
    """An unplanned exception is not retryable - retrying a bug just burns
    attempts and delays the failure the operator needs to see."""
    stage = _fake_stage("tts", raises=ZeroDivisionError("bug"))
    message = run_stage(stage, _ctx(job_id="j1", correlation_id="c1"), store)

    assert isinstance(message, Error)
    assert message.kind is ErrorKind.FATAL
    assert "ZeroDivisionError" in message.message


def test_a_stage_that_claims_an_artifact_it_never_staged_becomes_a_FATAL_Error(
    store: CasStore,
) -> None:
    """A stage's own bytes never reached the CAS - a bug, a swallowed
    exception, a half-written file. Without this check that would be reported
    as a clean Result, get recorded by the dispatcher, and only surface much
    later as a cache hit for an artifact that was never there."""
    stage = _fake_stage("tts", produces=[("narration", b"audio")])  # no cas: never staged

    message = run_stage(stage, _ctx(job_id="j1", correlation_id="c1"), store)

    assert isinstance(message, Error)
    assert message.kind is ErrorKind.FATAL
    assert "narration" in message.message


def test_run_stage_writes_no_database_rows(store: CasStore, db_conn: sqlite3.Connection) -> None:
    """The runner is database-pure: that is what lets a worker call it with no
    connection, and what keeps every write in the dispatcher."""
    before = db_conn.execute("SELECT count(*) FROM cas_objects").fetchone()[0]
    run_stage(
        _fake_stage("tts", produces=[("narration", b"audio")], cas=store),
        _ctx(job_id="j1", correlation_id="c1"),
        store,
    )
    assert db_conn.execute("SELECT count(*) FROM cas_objects").fetchone()[0] == before
