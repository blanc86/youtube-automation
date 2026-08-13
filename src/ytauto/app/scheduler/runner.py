"""The stage runner: execute exactly one pipeline stage.

Gathers a stage's upstream artifacts, computes its fingerprint spec, and runs
it, reporting what happened as a worker-protocol message. Deliberately
**database-pure**: it reads no rows and writes none. The dispatcher (Task 13)
is the only component that writes job state - keeping every read and write
out of here is what makes this testable without a database at all, and what
lets a worker subprocess call it with no SQLite connection.

The CAS boundary is split the same way ``CasStore`` itself is (Task 8): a
concrete ``Stage`` implementation lives outside ``core`` - in ``providers/``
or ``app/``, wherever it is constructed - and is given its own ``CasStore``
reference at construction time, which it uses to write its output bytes via
``stage_file``. ``core.pipeline.stage.Stage`` stays exactly as committed: a
two-argument ``run(ctx, emit)`` with no infra dependency, because ``core``
can never import ``infra``. ``run_stage`` itself never writes a blob or a
row. What it does instead is *verify*: once a stage returns, every artifact
it claims to have produced must actually exist in the CAS before this
reports success. A stage that lies about its own output - a bug, a swallowed
exception, a half-written file - would otherwise be recorded as a clean
success and only surface much later as a cache hit for a blob that was never
there.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from ytauto.app.scheduler.worker_protocol import ArtifactLine, Error, Result
from ytauto.core.errors import ErrorKind, ProviderError, ValidationError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.pipeline.fingerprint import FingerprintSpec
from ytauto.core.pipeline.graph import Pipeline
from ytauto.core.pipeline.stage import JobContext, Stage
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.cas.store import CasStore


@dataclass(frozen=True)
class RunnerContext:
    """Everything ``run_stage`` needs beyond the stage itself.

    ``job`` is the domain ``JobContext`` a stage's own business logic sees.
    ``correlation_id`` deliberately does not live on ``JobContext``: it is a
    protocol/tracing concern (see ``worker_protocol`` and ``infra.logging``),
    not something a stage's logic has any reason to see, and ``core`` has no
    business carrying a field that exists only to serve the worker pipe.

    ``job_id`` is duplicated here rather than read through ``job.job_id`` so
    callers addressing a protocol message do not have to reach through the
    domain object to get it - but two sources of truth for the same value is
    a real footgun, so the two are checked to agree rather than trusted to.

    Raises:
        ValidationError: if ``job_id`` does not match ``job.job_id``.
    """

    job: JobContext
    job_id: str
    correlation_id: str

    def __post_init__(self) -> None:
        if self.job_id != self.job.job_id:
            raise ValidationError(
                f"RunnerContext.job_id {self.job_id!r} does not match "
                f"job.job_id {self.job.job_id!r}"
            )


def gather_inputs(
    pipeline: Pipeline,
    stage_id: str,
    stage_fingerprints: Mapping[str, str],
    artifact_store: ArtifactStore,
) -> Mapping[str, tuple[ArtifactRef, ...]]:
    """Collect every upstream stage's recorded artifacts for ``stage_id``.

    Walks ``pipeline.upstream_of(stage_id)``, which is transitive, so a stage
    sees its grandparents' artifacts too, not only its direct parents'. An
    upstream stage absent from ``stage_fingerprints``, or whose fingerprint
    has nothing recorded (evicted, or genuinely not yet run - a resume),
    contributes nothing rather than raising: the runner is asked for what
    exists, not for what should eventually exist.

    Raises:
        ValidationError: if ``stage_id`` names no stage in ``pipeline`` (from
            ``Pipeline.upstream_of``), or if a fingerprint recorded in
            ``stage_fingerprints`` is malformed (from ``ArtifactStore.lookup``).
    """
    inputs: dict[str, tuple[ArtifactRef, ...]] = {}
    for upstream_id in pipeline.upstream_of(stage_id):
        fingerprint = stage_fingerprints.get(upstream_id)
        if fingerprint is None:
            continue
        artifacts = artifact_store.lookup(fingerprint)
        if artifacts is None:
            continue
        inputs[upstream_id] = artifacts
    return inputs


def build_spec(
    stage: Stage,
    provider_id: str,
    provider_version: str,
    inputs: Mapping[str, tuple[ArtifactRef, ...]],
    settings: Mapping[str, object],
) -> FingerprintSpec:
    """Build the ``FingerprintSpec`` for one execution of ``stage``.

    Flattens ``inputs`` into ``input_digests`` sorted by ``(stage_id,
    artifact_name)``. Stating this rule here is the point: ``inputs`` is a
    ``Mapping``, and a plain iteration over it would make the same inputs
    fingerprint differently depending on dict insertion order, which can
    differ between processes and silently disables caching across them.
    Sorting each stage's own artifacts by name is equally load-bearing for a
    different reason: two stage authors declaring the same inputs in a
    different order must still fingerprint identically.

    Raises:
        Nothing itself. ``compute_fingerprint``, called later against the
        returned spec, is what raises ``ValidationError`` if ``settings`` or
        an input digest turns out not to be fingerprintable.
    """
    flattened = tuple(
        (artifact.name, artifact.digest)
        for _stage_id, artifacts in sorted(inputs.items(), key=lambda kv: kv[0])
        for artifact in sorted(artifacts, key=lambda a: a.name)
    )
    return FingerprintSpec(
        stage_id=stage.id,
        stage_version=stage.version,
        provider_id=provider_id,
        provider_version=provider_version,
        input_digests=flattened,
        settings=settings,
    )


def _discard_progress(fraction: float, note: str) -> None:
    """The progress callback ``run_stage`` gives a stage.

    ``run_stage`` returns a single terminal ``Result | Error``, not a
    stream, so it has no channel to relay progress through - that is the
    worker entry point's job (Task 13), which owns writing ``Progress``
    lines to stdout as they happen. Progress is advisory only (see
    ``worker_protocol.Progress``) and a lost line changes nothing, so
    discarding it here keeps ``Stage.run`` free to call ``emit()`` without
    knowing, or needing to know, whether anyone is listening.
    """


def run_stage(stage: Stage, ctx: RunnerContext, cas: CasStore) -> Result | Error:
    """Run one stage and translate the outcome into a worker-protocol message.

    Division of responsibility, spelled out because the worker entry point
    (Task 13) depends on it: the stage writes its own output bytes through
    its own injected ``CasStore`` (a concrete ``Stage`` lives outside
    ``core`` precisely so it can hold that reference without ``core``
    importing ``infra``). ``run_stage`` never writes anything itself, to the
    database or the filesystem. What it does instead is verify: once
    ``stage.run`` returns, every artifact it claims now exists in ``cas``
    before this reports success. A stage that claims bytes it never wrote -
    a bug, a swallowed exception, a half-written file - would otherwise be
    reported as a clean ``Result``, get recorded by the dispatcher, and only
    surface later as a cache hit for a blob that was never there.

    A ``ProviderError`` becomes an ``Error`` carrying that error's own
    ``kind`` - the dispatcher needs it to tell a rate limit from a real
    failure. Any other exception, including a missing artifact caught by the
    verification above, becomes ``ErrorKind.FATAL``: retrying an unplanned
    failure only burns the job's attempts and delays the failure an operator
    needs to see.

    Raises:
        Nothing. Every exception ``stage.run`` raises is caught and
        translated into an ``Error`` message.
    """
    try:
        result = stage.run(ctx.job, _discard_progress)
    except ProviderError as exc:
        return Error(
            job_id=ctx.job_id,
            stage_id=stage.id,
            correlation_id=ctx.correlation_id,
            message=str(exc),
            kind=exc.kind,
            retry_after_s=exc.retry_after_s,
        )
    except Exception as exc:
        return Error(
            job_id=ctx.job_id,
            stage_id=stage.id,
            correlation_id=ctx.correlation_id,
            message=f"{type(exc).__name__}: {exc}",
            kind=ErrorKind.FATAL,
        )

    for artifact in result.artifacts:
        if not cas.exists(artifact.digest):
            return Error(
                job_id=ctx.job_id,
                stage_id=stage.id,
                correlation_id=ctx.correlation_id,
                message=(
                    f"stage {stage.id!r} claims artifact {artifact.name!r} "
                    f"(digest {artifact.digest}) but no such blob exists in the store"
                ),
                kind=ErrorKind.FATAL,
            )

    return Result(
        job_id=ctx.job_id,
        stage_id=stage.id,
        correlation_id=ctx.correlation_id,
        artifacts=tuple(
            ArtifactLine(name=a.name, kind=a.kind, digest=a.digest) for a in result.artifacts
        ),
        meta=result.meta,
    )
