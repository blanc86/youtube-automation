"""Stage resolution through ``importlib.metadata`` entry points.

Entry points are monkeypatched rather than installed for real: a unit test
that depended on the current state of the venv's metadata would pass or fail
depending on whether someone had re-run ``pip install -e .``, which is the
opposite of a hermetic test. The *real* discovery path - installed metadata,
a factory loaded across a process boundary - is exercised by the integration
suite, which spawns genuine workers against the distribution metadata under
``tests/``.

``EntryPoint`` objects here are the real class, not doubles, and their values
point back into this module, so ``EntryPoint.load()`` does its actual import
work rather than being stubbed out.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest

import ytauto.app.registry as registry_module
from ytauto.app.registry import build_pipeline, build_stage
from ytauto.core.errors import ValidationError
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.infra.cas.store import CasStore

# db_conn is defined in tests/unit/conftest.py.

_GROUP = "ytauto.stages"


class _RecordingStage:
    """A stage that remembers what its factory was handed.

    The reflection placeholder this registry replaces could only zero-arg
    construct a class, which is why the integration stages had to smuggle
    their CAS root in through an environment variable. That a stage now
    *receives* its CasStore is the point, so it is asserted on.
    """

    def __init__(self, stage_id: str, cas: CasStore, settings: Mapping[str, object]) -> None:
        self.id = stage_id
        self.version = 1
        self.depends_on: tuple[str, ...] = ()
        self.settings_keys: tuple[str, ...] = ()
        self.cas = cas
        self.settings = settings

    def fingerprint(self, ctx: JobContext) -> str:
        return "f" * 64

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        return StageResult(artifacts=())


def make_ingest_story(*, cas: CasStore, settings: Mapping[str, object]) -> _RecordingStage:
    return _RecordingStage("ingest_story", cas, settings)


def make_write_script(*, cas: CasStore, settings: Mapping[str, object]) -> _RecordingStage:
    return _RecordingStage("write_script", cas, settings)


def make_other(*, cas: CasStore, settings: Mapping[str, object]) -> _RecordingStage:
    return _RecordingStage("other", cas, settings)


def _entry(name: str, factory: str) -> EntryPoint:
    """A real EntryPoint pointing back into this module, so ``load()`` runs."""
    return EntryPoint(name=name, value=f"{__name__}:{factory}", group=_GROUP)


def _register(monkeypatch: pytest.MonkeyPatch, *entries: EntryPoint) -> None:
    """Replace the installed entry points with exactly ``entries``."""

    def _fake_entry_points(*, group: str) -> tuple[EntryPoint, ...]:
        assert group == _GROUP, f"the registry must look in {_GROUP!r}, not {group!r}"
        return entries

    monkeypatch.setattr(registry_module, "entry_points", _fake_entry_points)


@pytest.fixture()
def cas(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    return CasStore(root=tmp_path / "cas", conn=db_conn)


def test_build_stage_resolves_through_entry_points(
    cas: CasStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _register(monkeypatch, _entry("story_video:ingest_story", "make_ingest_story"))

    stage = build_stage("story_video", "ingest_story", cas, {"story_path": "x.txt"})

    assert stage.id == "ingest_story"


def test_a_resolved_stage_is_handed_its_cas_and_the_projects_settings(
    cas: CasStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reflection placeholder could not do this at all - it zero-arg
    constructed the class, so a stage's CAS root had to arrive through an
    environment variable. A stage writes its own output bytes; being given the
    store is what makes that possible."""
    _register(monkeypatch, _entry("story_video:ingest_story", "make_ingest_story"))

    stage = build_stage("story_video", "ingest_story", cas, {"voice": "en-GB-RyanNeural"})

    assert isinstance(stage, _RecordingStage)
    assert stage.cas is cas, "the stage must receive the very store the worker opened"
    assert stage.settings == {"voice": "en-GB-RyanNeural"}


def test_an_unknown_stage_id_names_what_was_available(cas: CasStore) -> None:
    """Deliberately *not* monkeypatched: this is the one test that runs
    against the venv's real installed metadata, so a change that broke
    discovery outright (a renamed group, a bad ``entry_points`` call) shows up
    here rather than only in the integration suite."""
    with pytest.raises(ValidationError, match="ingest_stroy"):
        build_stage("story_video", "ingest_stroy", cas, {})


def test_the_error_lists_what_is_registered(cas: CasStore, monkeypatch: pytest.MonkeyPatch) -> None:
    """A typo and a missing reinstall look identical from the call site; the
    list of registered names is what tells them apart."""
    _register(monkeypatch, _entry("story_video:ingest_story", "make_ingest_story"))

    with pytest.raises(ValidationError, match="story_video:ingest_story"):
        build_stage("story_video", "ingest_stroy", cas, {})


def test_a_stage_registered_under_another_pipeline_is_not_resolved(
    cas: CasStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The pipeline id is half the key, not decoration. Two pipelines may
    legitimately both have an ``ingest_story``, resolving to different
    factories."""
    _register(monkeypatch, _entry("story_video:ingest_story", "make_ingest_story"))

    with pytest.raises(ValidationError, match="shorts:ingest_story"):
        build_stage("shorts", "ingest_story", cas, {})


def test_build_pipeline_assembles_exactly_the_stages_registered_under_its_id(
    cas: CasStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _register(
        monkeypatch,
        _entry("story_video:ingest_story", "make_ingest_story"),
        _entry("story_video:write_script", "make_write_script"),
        _entry("shorts:other", "make_other"),
    )

    pipeline = build_pipeline("story_video", cas, {})

    assert pipeline.id == "story_video"
    assert sorted(stage.id for stage in pipeline.stages) == ["ingest_story", "write_script"]


def test_build_pipeline_rejects_a_pipeline_nothing_is_registered_for(
    cas: CasStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty pipeline would otherwise fail much later, inside
    ``Pipeline``'s own "pipeline is empty" check, naming the pipeline but not
    the reason."""
    _register(monkeypatch, _entry("shorts:other", "make_other"))

    with pytest.raises(ValidationError, match="story_video"):
        build_pipeline("story_video", cas, {})


def test_a_pipeline_name_is_matched_whole_not_as_a_prefix(
    cas: CasStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``story_video_shorts:other`` also starts with ``story_video``; without
    the separator in the prefix it would be pulled into ``story_video``'s
    pipeline and silently run an extra stage on every job."""
    _register(
        monkeypatch,
        _entry("story_video:ingest_story", "make_ingest_story"),
        _entry("story_video_shorts:other", "make_other"),
    )

    pipeline = build_pipeline("story_video", cas, {})

    assert [stage.id for stage in pipeline.stages] == ["ingest_story"]
