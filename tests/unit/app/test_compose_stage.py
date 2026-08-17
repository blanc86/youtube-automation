"""Unit tests for ``ComposeStage.run()``, driven with fake ffmpeg plumbing.

``infra.ffmpeg.locator.locate`` and ``infra.ffmpeg.probe.probe`` are
monkeypatched at the ``ytauto.app.stages.compose`` call site, and
``ComposeStage._run_ffmpeg`` itself is replaced with a scripted double, so
this suite never touches a real ffmpeg binary or spawns a subprocess - the
real render (and the real encoder fallback chain on this machine's actual
hardware) is ``tests/integration/test_compose.py``'s job. What is tested
here is everything Task 11's review found untested: the mandated
ffmpeg-failure path (Important #3), the encoder-fails-then-falls-back-to-
libx264 behaviour and its "never override an explicit choice" guard
(Important #4), the ``has_subtitle_burn_in`` pre-flight check bundled into
the same fix, the ``caption_style["font_size"]`` cache-key/behaviour
mismatch (Important #5), and the CAS-orphan ordering fix (Minor).

``db_conn`` is defined in ``tests/unit/conftest.py``.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest

import ytauto.app.stages.compose as compose_module
from ytauto.app.stages.compose import ComposeStage, make_compose_landscape
from ytauto.core.errors import ConfigurationError, ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.pipeline.stage import JobContext, ProgressFn
from ytauto.infra.cas.store import CasStore
from ytauto.infra.ffmpeg.locator import FfmpegBinaries
from ytauto.infra.ffmpeg.probe import FfmpegCapabilities

_NOOP_EMIT: ProgressFn = lambda fraction, note: None  # noqa: E731


def _binaries() -> FfmpegBinaries:
    return FfmpegBinaries(ffmpeg=Path("ffmpeg"), ffprobe=Path("ffprobe"), version="7.1.1")


def _capabilities(
    *,
    encoders: frozenset[str] = frozenset({"h264_nvenc", "libx264"}),
    filters: frozenset[str] = frozenset({"ass", "concat", "scale", "pad"}),
) -> FfmpegCapabilities:
    return FfmpegCapabilities(encoders=encoders, filters=filters)


class _FakeRun:
    """A scripted double for ``ComposeStage._run_ffmpeg``.

    Assigned directly onto the class (``ComposeStage._run_ffmpeg = fake``),
    not onto an instance - a plain callable object assigned as a class
    attribute is not bound the way a function is, so ``self._run_ffmpeg(a, b,
    c)`` calls ``fake(a, b, c)`` with no implicit ``self``, matching
    ``_run_ffmpeg``'s own ``(self, ffmpeg, args, cwd)`` signature minus the
    leading ``self``.

    One ``(returncode, stderr)`` pair is consumed per call, in order - a
    test scripts exactly as many outcomes as it expects calls, so an
    unexpected extra call fails loudly with ``IndexError`` rather than
    silently reusing the last outcome. On ``returncode == 0`` a placeholder
    file is written to the invocation's own output path (``argv[-1]``,
    always ``str(out_path)`` per ``compose_args``'s own contract) so
    ``ComposeStage.run``'s later ``stage_path(out_path, move=True)`` has a
    real file to move into the CAS.
    """

    def __init__(self, outcomes: Sequence[tuple[int, str]]) -> None:
        self._outcomes = list(outcomes)
        self.calls: list[list[str]] = []

    def __call__(
        self, ffmpeg: Path, args: list[str], cwd: Path
    ) -> subprocess.CompletedProcess[str]:
        argv = [str(ffmpeg), *args]
        self.calls.append(argv)
        returncode, stderr = self._outcomes.pop(0)
        if returncode == 0:
            Path(argv[-1]).write_bytes(b"fake rendered video bytes")
        return subprocess.CompletedProcess(argv, returncode, stdout="", stderr=stderr)


def _env(
    tmp_path: Path,
    db_conn: sqlite3.Connection,
    *,
    clip_ids: Sequence[str] = ("clip-a",),
    encoder: str = "auto",
    caption_style: dict[str, object] | None = None,
) -> tuple[CasStore, JobContext]:
    """A real ``CasStore`` plus a ``JobContext`` naming real, staged
    upstream artifacts - segments.json/timeline.json/narration.mp3/the
    manifest, in the exact wire shapes those stages actually write, per this
    task's own "read the real producer" discipline. ``narration.mp3`` and
    the manifest's video digests are placeholder bytes: nothing under test
    reads their content before a (faked) ffmpeg call, only their CAS paths.
    """
    cas = CasStore(root=tmp_path / "cas", conn=db_conn)

    segments_payload = [
        {"clip_id": clip_id, "in_point_s": 0.0, "duration_s": 1.0} for clip_id in clip_ids
    ]
    segments_digest = cas.stage_file(json.dumps(segments_payload).encode("utf-8"), kind="json")

    timeline_payload = {"duration_s": 1.0, "groups": [], "segments": []}
    timeline_digest = cas.stage_file(json.dumps(timeline_payload).encode("utf-8"), kind="json")

    narration_digest = cas.stage_file(b"fake narration bytes", kind="audio")

    manifest_payload = [
        {
            "clip_id": clip_id,
            "duration_s": 5.0,
            "source_width": 1920,
            "source_height": 1080,
            "normalised_landscape_digest": str(
                cas.stage_file(f"clip bytes for {clip_id}".encode(), kind="video")
            ),
            "normalised_vertical_digest": str(
                cas.stage_file(f"clip bytes for {clip_id}".encode(), kind="video")
            ),
        }
        for clip_id in dict.fromkeys(clip_ids)
    ]
    manifest_digest = cas.stage_file(
        json.dumps(manifest_payload).encode("utf-8"), kind="broll_manifest"
    )

    ctx = JobContext(
        job_id="job",
        project_id="proj",
        settings={
            "broll_manifest_digest": str(manifest_digest),
            "caption_style": caption_style if caption_style is not None else {},
            "encoder": encoder,
        },
        inputs={
            "plan_timeline": (
                ArtifactRef(name="timeline.json", kind="json", digest=timeline_digest),
            ),
            "select_broll": (
                ArtifactRef(name="segments.json", kind="json", digest=segments_digest),
            ),
            "synthesize_speech": (
                ArtifactRef(name="narration.mp3", kind="audio", digest=narration_digest),
            ),
        },
        workdir=tmp_path / "work",
    )
    return cas, ctx


def _patch_ffmpeg_discovery(
    monkeypatch: pytest.MonkeyPatch, *, capabilities: FfmpegCapabilities | None = None
) -> None:
    monkeypatch.setattr(compose_module, "locate", lambda: _binaries())
    monkeypatch.setattr(compose_module, "probe", lambda binaries: capabilities or _capabilities())


# -- gpu_pool (Important #2 - pinned directly, not only through the dispatcher) --


def test_compose_stage_declares_the_gpu_encode_pool() -> None:
    """Task 11's review: a typo here (``gpu_pol``) used to degrade silently
    to ``gpu_compute`` via ``Dispatcher._spawn``'s old ``getattr`` fallback,
    with no test catching it. Pinned directly on the class, independent of
    the dispatcher-level tests that exercise the *consequence*."""
    assert ComposeStage.gpu_pool == "gpu_encode"


# -- has_subtitle_burn_in pre-flight (bundled into Important #4) -----------------


def test_a_build_without_libass_raises_configuration_error_before_reading_anything(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Turns an opaque filter-graph error into a clear, early
    ``ConfigurationError`` - and proves "early" for real: ``ctx.inputs`` is
    deliberately empty here, so if the check ran *after* the first
    ``ctx.input(...)`` call this would instead raise ``ValidationError``
    ("no inputs from stage...")."""
    _patch_ffmpeg_discovery(monkeypatch, capabilities=_capabilities(filters=frozenset({"scale"})))
    cas = CasStore(root=tmp_path / "cas", conn=db_conn)
    ctx = JobContext(
        job_id="job",
        project_id="proj",
        settings={"broll_manifest_digest": "0" * 64, "caption_style": {}, "encoder": "auto"},
        inputs={},
        workdir=tmp_path / "work",
    )
    stage = make_compose_landscape(cas=cas, settings={})

    with pytest.raises(ConfigurationError, match="ass"):
        stage.run(ctx, _NOOP_EMIT)


# -- Important #3: the mandated ffmpeg-failure path had zero coverage -----------


def test_a_failed_ffmpeg_run_is_fatal_and_names_a_log_containing_the_real_stderr(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_ffmpeg_discovery(monkeypatch)
    cas, ctx = _env(tmp_path, db_conn, encoder="libx264")  # explicit: no fallback retry involved
    stage = make_compose_landscape(cas=cas, settings={})
    fake = _FakeRun([(1, "ffmpeg: [libx264] real diagnostic text here")])
    monkeypatch.setattr(ComposeStage, "_run_ffmpeg", fake)

    with pytest.raises(ProviderError) as exc_info:
        stage.run(ctx, _NOOP_EMIT)

    assert exc_info.value.kind is ErrorKind.FATAL
    assert fake.calls == [fake.calls[0]], "an explicit encoder must never be retried"

    log_path = tmp_path / "work" / "ffmpeg-stderr.log"
    assert str(log_path) in str(exc_info.value)
    assert log_path.is_file()
    assert "real diagnostic text here" in log_path.read_text(encoding="utf-8")


# -- Important #4: encoder fallback on a runtime failure, not just a listing check --


def test_a_hardware_encoder_s_runtime_failure_falls_back_to_libx264_on_auto(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The scenario the review named explicitly: h264_nvenc is *listed* but
    fails at runtime (driver mismatch, VRAM exhaustion, a concurrent NVENC
    session already held). With encoder == "auto" this must retry once with
    libx264 rather than failing the whole job."""
    _patch_ffmpeg_discovery(monkeypatch)  # best_h264_encoder() -> h264_nvenc (listed first)
    cas, ctx = _env(tmp_path, db_conn, encoder="auto")
    stage = make_compose_landscape(cas=cas, settings={})
    fake = _FakeRun(
        [
            (1, "h264_nvenc: driver mismatch"),
            (0, ""),
        ]
    )
    monkeypatch.setattr(ComposeStage, "_run_ffmpeg", fake)

    result = stage.run(ctx, _NOOP_EMIT)

    assert len(fake.calls) == 2, "must retry exactly once"
    assert "-c:v" in fake.calls[0] and "h264_nvenc" in fake.calls[0]
    assert "-c:v" in fake.calls[1] and "libx264" in fake.calls[1]
    assert result.artifact("master_1920x1080.mp4") is not None


def test_an_explicit_encoder_choice_is_never_overridden_by_the_fallback(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retrying past an operator's own explicit choice would silently
    ignore it - the one case the fallback must never engage."""
    _patch_ffmpeg_discovery(monkeypatch)
    cas, ctx = _env(tmp_path, db_conn, encoder="h264_nvenc")
    stage = make_compose_landscape(cas=cas, settings={})
    fake = _FakeRun([(1, "h264_nvenc: driver mismatch")])
    monkeypatch.setattr(ComposeStage, "_run_ffmpeg", fake)

    with pytest.raises(ProviderError) as exc_info:
        stage.run(ctx, _NOOP_EMIT)

    assert len(fake.calls) == 1, "an explicit encoder must not be retried with a different one"
    assert exc_info.value.kind is ErrorKind.FATAL
    assert "h264_nvenc" in str(exc_info.value)


def test_when_both_the_primary_and_the_fallback_encoder_fail_the_log_names_both(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_ffmpeg_discovery(monkeypatch)
    cas, ctx = _env(tmp_path, db_conn, encoder="auto")
    stage = make_compose_landscape(cas=cas, settings={})
    fake = _FakeRun(
        [
            (1, "h264_nvenc: driver mismatch"),
            (1, "libx264: also broken somehow"),
        ]
    )
    monkeypatch.setattr(ComposeStage, "_run_ffmpeg", fake)

    with pytest.raises(ProviderError) as exc_info:
        stage.run(ctx, _NOOP_EMIT)

    assert len(fake.calls) == 2
    assert exc_info.value.kind is ErrorKind.FATAL
    log_text = (tmp_path / "work" / "ffmpeg-stderr.log").read_text(encoding="utf-8")
    assert "h264_nvenc: driver mismatch" in log_text
    assert "libx264: also broken somehow" in log_text


def test_the_fallback_is_not_attempted_when_auto_already_resolved_to_libx264(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No hardware encoder available at all (CI's macOS runners): auto
    resolves straight to libx264, which then fails - retrying with the
    identical encoder again would be pointless."""
    _patch_ffmpeg_discovery(
        monkeypatch, capabilities=_capabilities(encoders=frozenset({"libx264"}))
    )
    cas, ctx = _env(tmp_path, db_conn, encoder="auto")
    stage = make_compose_landscape(cas=cas, settings={})
    fake = _FakeRun([(1, "libx264: broken")])
    monkeypatch.setattr(ComposeStage, "_run_ffmpeg", fake)

    with pytest.raises(ProviderError):
        stage.run(ctx, _NOOP_EMIT)

    assert len(fake.calls) == 1


# -- Important #5: caption_style["font_size"] must survive, not just render_ass's own default --


def test_an_explicit_caption_style_font_size_is_honoured_not_overwritten(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Overwriting font_size unconditionally made it a declared
    settings_keys member the fingerprint depended on while the code
    guaranteed it never actually changed the rendered output - setting it
    to 72 forced a full re-render for a byte-identical 54px result. Proven
    on the real staged .ass artifact, not just the style dict."""
    _patch_ffmpeg_discovery(monkeypatch)
    cas, ctx = _env(tmp_path, db_conn, caption_style={"font_size": 72})
    stage = make_compose_landscape(cas=cas, settings={})
    monkeypatch.setattr(ComposeStage, "_run_ffmpeg", _FakeRun([(0, "")]))

    result = stage.run(ctx, _NOOP_EMIT)

    ass_text = cas.read_bytes(result.artifact("captions.ass").digest).decode("utf-8")
    assert "Style: Default,Arial,72," in ass_text
    assert "Style: Default,Arial,54," not in ass_text


def test_a_missing_caption_style_font_size_falls_back_to_the_canvas_derived_value(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The other half of the same fix: never rely on render_ass's own
    canvas-agnostic default (96) when the caller supplies nothing at all."""
    _patch_ffmpeg_discovery(monkeypatch)
    cas, ctx = _env(tmp_path, db_conn, caption_style={})
    stage = make_compose_landscape(cas=cas, settings={})
    monkeypatch.setattr(ComposeStage, "_run_ffmpeg", _FakeRun([(0, "")]))

    result = stage.run(ctx, _NOOP_EMIT)

    ass_text = cas.read_bytes(result.artifact("captions.ass").digest).decode("utf-8")
    assert "Style: Default,Arial,54," in ass_text, "1080-tall landscape: height // 20 == 54"


# -- Minor: the .ass blob must not be staged into the CAS before ffmpeg succeeds --


def test_the_ass_blob_is_not_staged_into_the_cas_on_a_failed_render(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Staging it before the ffmpeg call left an orphaned blob file (no
    ``cas_objects`` row, so the evictor - which walks DB rows, not the
    filesystem - could never reclaim it) on every FATAL failure."""
    _patch_ffmpeg_discovery(monkeypatch)
    cas, ctx = _env(tmp_path, db_conn, encoder="libx264")
    staged_kinds: list[str] = []
    real_stage_file = cas.stage_file

    def _spying_stage_file(data: bytes, *, kind: str) -> object:
        staged_kinds.append(kind)
        return real_stage_file(data, kind=kind)

    monkeypatch.setattr(cas, "stage_file", _spying_stage_file)
    stage = make_compose_landscape(cas=cas, settings={})
    monkeypatch.setattr(ComposeStage, "_run_ffmpeg", _FakeRun([(1, "boom")]))

    with pytest.raises(ProviderError):
        stage.run(ctx, _NOOP_EMIT)

    assert "text" not in staged_kinds, (
        "captions.ass (kind='text') must not be staged into the CAS before ffmpeg succeeds"
    )


# -- whole-branch review, Important #4: no full-size duplicate outside the CAS --


def test_the_master_is_moved_into_the_cas_not_left_behind_in_the_workdir(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ffmpeg has to write to a real path, and that path is ``ctx.workdir``.
    Before this fix the render was then read into memory in full and staged
    with ``stage_file``, leaving the whole master in
    ``<data>/assets/work/<job>/<stage>/`` forever - nothing in ``src/`` ever
    removed that tree, and ``Evictor`` walks ``cas_root``/``cas_objects``
    only, so the two largest files per render sat entirely outside the 40 GiB
    ceiling.

    The ``.ass`` is deliberately still there: it is small, and the workdir is
    where the diagnostics live (see ``run``'s docstring on why the directory
    itself is never cleared here).
    """
    _patch_ffmpeg_discovery(monkeypatch)
    cas, ctx = _env(tmp_path, db_conn)
    stage = make_compose_landscape(cas=cas, settings={})
    monkeypatch.setattr(ComposeStage, "_run_ffmpeg", _FakeRun([(0, "")]))

    result = stage.run(ctx, _NOOP_EMIT)

    master = result.artifact("master_1920x1080.mp4")
    assert cas.exists(master.digest), "the master must be in the CAS"
    assert not (ctx.workdir / "master_1920x1080.mp4").exists(), (
        "the workdir must not keep a full-size duplicate of the master"
    )
    assert (ctx.workdir / "captions.ass").is_file(), (
        "the small workdir files must survive - only the staged media is removed"
    )


def test_a_failed_render_keeps_everything_in_the_workdir_for_diagnosis(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reason the cleanup is one file on the success path rather than a
    directory wipe. A failed render is exactly the case whose ``ffmpeg-stderr.log``
    someone needs, and whose partial output may be the evidence of *how* it
    failed - so the failure path removes nothing at all.
    """
    _patch_ffmpeg_discovery(monkeypatch)
    cas, ctx = _env(tmp_path, db_conn, encoder="libx264")
    stage = make_compose_landscape(cas=cas, settings={})
    monkeypatch.setattr(ComposeStage, "_run_ffmpeg", _FakeRun([(1, "boom")]))

    with pytest.raises(ProviderError):
        stage.run(ctx, _NOOP_EMIT)

    assert (ctx.workdir / "ffmpeg-stderr.log").is_file()
    assert "boom" in (ctx.workdir / "ffmpeg-stderr.log").read_text(encoding="utf-8")
    assert (ctx.workdir / "captions.ass").is_file()
