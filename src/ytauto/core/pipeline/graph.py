"""The stage DAG: validation, deterministic ordering, and invalidation."""

from __future__ import annotations

from dataclasses import dataclass, field

from ytauto.core.errors import ValidationError
from ytauto.core.models.names import assert_unique_names
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
        assert_unique_names(ids, what="stage", context=f"pipeline {self.id!r}")

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
        """Dependencies first, via depth-first search seeded in stage-ID order.

        This is deterministic - two runs of the same pipeline plan identically,
        which matters because a varying order could vary the artifacts fed into
        a downstream fingerprint - but it is DFS post-order, not a global
        lexicographic tiebreak: independent stages are not necessarily returned
        in ID order relative to each other. See ``ready_stages`` for a view that
        does expose which stages are mutually independent.
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

    def ready_stages(self, done: frozenset[str]) -> tuple[Stage, ...]:
        """Stages whose dependencies are all satisfied and which are not yet done.

        ``topological_order`` gives a total order and so cannot express that two
        stages are independent. The governor needs exactly that: with
        ``gpu_compute`` capacity 1, the point is to run non-GPU stages alongside
        the single GPU stage. Ordered by stage id so a dispatcher's choice under
        capacity pressure is reproducible.
        """
        return tuple(
            stage
            for stage in sorted(self.stages, key=lambda s: s.id)
            if stage.id not in done and set(stage.depends_on) <= done
        )

    def upstream_of(self, stage_id: str) -> frozenset[str]:
        """Every stage ``stage_id`` transitively depends on. The mirror of downstream_of.

        This is what populates ``JobContext.inputs`` on a resume: the runner has
        to gather the artifacts of everything upstream, not just direct parents.

        Raises:
            ValidationError: if ``stage_id`` names no stage in this pipeline.
        """
        if stage_id not in self._by_id:
            raise ValidationError(f"unknown stage {stage_id!r} in pipeline {self.id!r}")
        seen: set[str] = set()
        frontier = list(self._by_id[stage_id].depends_on)
        while frontier:
            current = frontier.pop()
            if current in seen:
                continue
            seen.add(current)
            frontier.extend(self._by_id[current].depends_on)
        return frozenset(seen)
