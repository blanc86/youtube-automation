"""The stage DAG: validation, deterministic ordering, and invalidation."""

from __future__ import annotations

from dataclasses import dataclass, field

from ytauto.core.errors import ValidationError
from ytauto.core.pipeline.stage import Stage


@dataclass(frozen=True)
class Pipeline:
    """A validated directed acyclic graph of stages.

    Validation happens once, at construction, so nothing downstream has to
    re-check for cycles or dangling dependencies.

    Raises:
        ValidationError: if the pipeline is empty, has duplicate stage IDs,
            references an unknown dependency, or contains a cycle.
    """

    id: str
    stages: tuple[Stage, ...]
    _by_id: dict[str, Stage] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not self.stages:
            raise ValidationError(f"pipeline {self.id!r} is empty")

        ids = [stage.id for stage in self.stages]
        if len(ids) != len(set(ids)):
            duplicates = sorted({i for i in ids if ids.count(i) > 1})
            raise ValidationError(f"duplicate stage ids: {duplicates}")

        known = set(ids)
        for stage in self.stages:
            unknown = sorted(set(stage.depends_on) - known)
            if unknown:
                raise ValidationError(f"stage {stage.id!r} depends on unknown stage(s): {unknown}")

        object.__setattr__(self, "_by_id", {stage.id: stage for stage in self.stages})
        self._assert_acyclic()

    def _assert_acyclic(self) -> None:
        """Detect cycles via depth-first search with a recursion stack.

        Raises:
            ValidationError: if any stage participates in a dependency cycle.
        """
        visiting: set[str] = set()
        done: set[str] = set()

        def visit(stage_id: str, trail: tuple[str, ...]) -> None:
            if stage_id in done:
                return
            if stage_id in visiting:
                cycle = " -> ".join((*trail, stage_id))
                raise ValidationError(f"pipeline {self.id!r} contains a cycle: {cycle}")
            visiting.add(stage_id)
            for dependency in sorted(self._by_id[stage_id].depends_on):
                visit(dependency, (*trail, stage_id))
            visiting.discard(stage_id)
            done.add(stage_id)

        for stage in self.stages:
            visit(stage.id, ())

    def stage_by_id(self, stage_id: str) -> Stage:
        """Look up one stage.

        Raises:
            ValidationError: if no stage has that ID.
        """
        stage = self._by_id.get(stage_id)
        if stage is None:
            raise ValidationError(
                f"no stage {stage_id!r} in pipeline {self.id!r}; known: {sorted(self._by_id)}"
            )
        return stage

    def topological_order(self) -> tuple[Stage, ...]:
        """Dependencies first; ties broken by stage ID.

        The tiebreak makes ordering independent of declaration order, so two
        runs of the same pipeline plan identically. A varying order could vary
        the artifacts fed into a downstream fingerprint.
        """
        ordered: list[Stage] = []
        placed: set[str] = set()

        def place(stage_id: str) -> None:
            if stage_id in placed:
                return
            for dependency in sorted(self._by_id[stage_id].depends_on):
                place(dependency)
            placed.add(stage_id)
            ordered.append(self._by_id[stage_id])

        for stage_id in sorted(self._by_id):
            place(stage_id)
        return tuple(ordered)

    def downstream_of(self, stage_id: str) -> frozenset[str]:
        """Every stage that transitively depends on this one, excluding itself.

        This is what turns "the script changed" into "rerun these stages and
        reuse the rest".

        Raises:
            ValidationError: if no stage has that ID.
        """
        self.stage_by_id(stage_id)
        affected: set[str] = set()
        frontier = {stage_id}
        while frontier:
            nxt = {
                stage.id
                for stage in self.stages
                if set(stage.depends_on) & frontier and stage.id not in affected
            }
            affected |= nxt
            frontier = nxt
        affected.discard(stage_id)
        return frozenset(affected)
