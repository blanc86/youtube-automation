"""Stage resolution by entry point: the one place a pipeline becomes objects.

Nothing in the schema stores a serialised pipeline - a job's DAG is code -
and until now the dispatcher's caller was expected to hand it a
``Mapping[str, Pipeline]`` it had assembled itself, while the worker
subprocess re-imported the stage class by reflection off a
``"module:QualName"`` string the assignment carried. Both halves are replaced
here by one mechanism: an entry point named ``"<pipeline_id>:<stage_id>"``
under the ``ytauto.stages`` group, whose value is a factory taking ``cas`` and
``settings``.

**Resolution is dynamic on purpose, and the import-linter gate is why.**
``pyproject.toml`` declares a forbidden contract - "app depends only on core
and infra" - listing ``ytauto.providers``. A registry that did
``from ytauto.providers.tts import EdgeTtsStage`` here would break that
contract on the very first stage. ``importlib.metadata.entry_points`` is
invisible to static analysis, so the dependency goes the right way round: a
provider package depends on ``ytauto.core`` and announces itself, and neither
``core`` nor ``app`` ever names it. That is also exactly what
``core/ports/providers.py``'s own module docstring already promises - "a new
TTS engine is added by implementing the relevant Protocol and registering an
entry point, with no change to core/ or app/".

**A factory, not a class.** A real stage needs its ``CasStore`` (it writes its
own output bytes - see ``scheduler/runner.py``) and its project settings at
construction time, which is precisely what the reflection placeholder could
not express: it zero-arg-constructed the class and left the CAS root to be
smuggled in through an environment variable. Entry-point values therefore
point at a callable ``factory(*, cas, settings) -> Stage``, not at the class.

**Entry points need a reinstall.** They are read from installed distribution
metadata, not from ``pyproject.toml`` on disk, so adding one to the table and
not re-running ``pip install -e ".[dev]"`` leaves it undiscoverable. The
integration suite instead ships its own small distribution metadata
directory under ``tests/`` (see ``tests/ytauto_it_stages-0.0.0.dist-info``),
which both proves this path works for a third-party plugin and keeps test
stages out of the shipped package's metadata.
"""

from __future__ import annotations

from collections.abc import Mapping
from importlib.metadata import EntryPoint, entry_points

from ytauto.core.errors import ValidationError
from ytauto.core.pipeline.graph import Pipeline
from ytauto.core.pipeline.stage import Stage
from ytauto.infra.cas.store import CasStore

_GROUP = "ytauto.stages"
"""The entry-point group every stage factory registers under."""


def _registered() -> dict[str, EntryPoint]:
    """Every registered stage entry point, keyed by ``"<pipeline_id>:<stage_id>"``.

    Not cached: entry points are read from installed metadata, and a cache
    here would mean a freshly installed provider package stayed invisible
    until the process restarted. Discovery costs one filesystem scan of
    ``sys.path``, and a worker resolves exactly one stage per process.
    """
    return {ep.name: ep for ep in entry_points(group=_GROUP)}


def build_stage(
    pipeline_id: str, stage_id: str, cas: CasStore, settings: Mapping[str, object]
) -> Stage:
    """Construct one stage by entry-point name.

    ``settings`` is the project's *whole* settings mapping, not a projection:
    a stage narrows it itself through its own ``settings_keys`` when it
    fingerprints (see ``app.stage_support``), and a factory may legitimately
    read a key it does not fingerprint on.

    **A stage's ``fingerprint`` must be a pure function of its
    ``JobContext``, never of anything this factory decided.** This function is
    called twice for one stage execution, in two processes, with two different
    ``settings`` arguments: the dispatcher builds its pipeline once per
    process from whatever its caller passed (``app.registry.build_pipeline``),
    and the worker builds the stage again per job from that job's real
    settings. The dispatcher's copy is the one whose digest gets recorded. A
    factory that baked a settings-derived decision - a provider chosen from
    ``settings["tts_engine"]``, say - into something the stage fingerprints on
    would have the two disagree, and the stage's output would be indexed under
    a digest the executed configuration never reproduces. Anything that
    belongs in the fingerprint is read from ``ctx.settings`` at fingerprint
    time, through ``settings_keys``. ``app/worker.py``'s
    ``_fingerprint_disagreement`` refuses to run a stage where this was got
    wrong, so the failure is loud rather than a poisoned cache - but it fails
    the job, which is not a substitute for writing the stage correctly.

    Raises:
        ValidationError: no entry point is registered under this name. The
            message names what *is* registered, because the overwhelmingly
            likely cause is a typo or a package that was added to the entry
            point table without a reinstall - both of which are invisible
            otherwise.
    """
    name = f"{pipeline_id}:{stage_id}"
    found = _registered()
    entry = found.get(name)
    if entry is None:
        raise ValidationError(f"no stage registered as {name!r}; registered: {sorted(found)}")
    factory = entry.load()
    stage: Stage = factory(cas=cas, settings=settings)
    return stage


def build_pipeline(pipeline_id: str, cas: CasStore, settings: Mapping[str, object]) -> Pipeline:
    """Assemble every stage registered under ``pipeline_id`` into a ``Pipeline``.

    Membership is the entry-point table, not a hardcoded list: a pipeline is
    exactly the stages registered under its id, so adding a stage is one
    table entry rather than an edit here as well.

    Stages are constructed in sorted id order for determinism, though nothing
    downstream depends on it - ``Pipeline`` re-derives its own order from
    ``depends_on`` (see ``ready_stages``).

    Raises:
        ValidationError: no stage is registered for this pipeline at all, or
            the resulting set of stages is not a valid DAG - a dangling
            ``depends_on`` (a stage of this pipeline that was never
            registered) or a cycle, both from ``Pipeline``.
    """
    prefix = f"{pipeline_id}:"
    names = sorted(name for name in _registered() if name.startswith(prefix))
    if not names:
        raise ValidationError(
            f"no stages registered for pipeline {pipeline_id!r}; "
            f"registered: {sorted(_registered())}"
        )
    stages = tuple(build_stage(pipeline_id, name[len(prefix) :], cas, settings) for name in names)
    return Pipeline(id=pipeline_id, stages=stages)
