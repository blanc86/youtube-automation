"""The contract every pipeline stage implements."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable

from ytauto.core.errors import ValidationError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.names import assert_unique_names

ProgressFn = Callable[[float, str], None]
"""Report progress as (fraction 0.0-1.0, human-readable message)."""


@dataclass(frozen=True)
class JobContext:
    """Everything a stage may see about the job it is running for.

    ``workdir`` is the one place a filesystem path legitimately reaches a
    stage. It must never reach a fingerprint - see core.pipeline.fingerprint.
    """

    job_id: str
    project_id: str
    settings: Mapping[str, object]
    inputs: Mapping[str, tuple[ArtifactRef, ...]]
    workdir: Path

    def input(self, stage_id: str, name: str) -> ArtifactRef:
        """Fetch one named artifact produced by an upstream stage.

        Raises:
            ValidationError: if the stage produced no artifacts for this job, or
                produced none by that name.
        """
        produced = self.inputs.get(stage_id)
        if produced is None:
            raise ValidationError(
                f"no inputs from stage {stage_id!r}; available: {sorted(self.inputs)}"
            )
        for artifact in produced:
            if artifact.name == name:
                return artifact
        raise ValidationError(
            f"stage {stage_id!r} produced no artifact named {name!r}; "
            f"available: {sorted(a.name for a in produced)}"
        )


@dataclass(frozen=True)
class StageResult:
    """What a stage hands back: named artifacts plus optional metadata.

    Raises:
        ValidationError: if two artifacts share a name.
    """

    artifacts: tuple[ArtifactRef, ...]
    meta: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        names = [a.name for a in self.artifacts]
        assert_unique_names(names, what="artifact", context="a stage result")

    def artifact(self, name: str) -> ArtifactRef:
        """Fetch one artifact by name.

        Raises:
            ValidationError: if no artifact has that name.
        """
        for artifact in self.artifacts:
            if artifact.name == name:
                return artifact
        raise ValidationError(
            f"no artifact named {name!r}; produced: {sorted(a.name for a in self.artifacts)}"
        )


@runtime_checkable
class Stage(Protocol):
    """One node of the pipeline DAG.

    ``fingerprint`` must be a pure function of the context: same inputs and
    settings, same digest, across processes and interpreter restarts. The
    scheduler skips any stage whose fingerprint already has stored artifacts,
    so an unstable fingerprint silently disables all caching.
    """

    @property
    def id(self) -> str:
        """Stable identifier, unique within a pipeline."""

    @property
    def version(self) -> int:
        """Bump when the stage's behaviour changes, to invalidate old artifacts."""

    @property
    def depends_on(self) -> tuple[str, ...]:
        """IDs of stages whose artifacts this one consumes."""

    def fingerprint(self, ctx: JobContext) -> str:
        """Content hash of everything that determines this stage's output."""

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        """Do the work. Called only when the fingerprint missed."""
