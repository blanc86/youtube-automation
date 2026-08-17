"""Integration test: ``compose_landscape`` against a real ffmpeg.

``ComposeStage`` is driven directly, not through the dispatcher - the same
choice ``tests/integration/test_broll_ingest.py`` makes for
``BrollLibrary.add``, and for the same reason: this is about proving the
real ffmpeg filter graph renders the right thing, not about re-exercising
the worker/dispatcher plumbing other integration tests already cover.
``db_conn``/``cas`` are defined locally rather than added to
``tests/integration/conftest.py``, mirroring ``test_broll_ingest.py`` again.

Every input this stage reads is built by hand here, in the exact wire shapes
the real upstream stages write them in (``segments.json``'s
``{"clip_id", "in_point_s", "duration_s"}``, ``timeline.json``'s
``json.dumps(asdict(timeline))``, the manifest's
``{"clip_id", ..., "normalised_landscape_digest", ...}``) - confirmed
against ``src/ytauto/app/stages/select_broll.py``,
``src/ytauto/app/stages/plan_timeline.py`` and ``src/ytauto/infra/broll.py``
before writing this test, per this task's own instruction to read each
upstream contract rather than trust a summary.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from ytauto.app.stages.compose import make_compose_landscape
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.pipeline.stage import JobContext
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations
from ytauto.infra.ffmpeg.locator import FfmpegBinaries, locate
from ytauto.infra.ffmpeg.media_probe import probe_dimensions

pytestmark = pytest.mark.integration


# -- fixtures -----------------------------------------------------------------


@pytest.fixture()
def db_conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """A migrated database. Closed on teardown so Windows can delete tmp_path."""
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def cas(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    return CasStore(root=tmp_path / "cas", conn=db_conn)


@pytest.fixture()
def ffmpeg_binaries() -> FfmpegBinaries:
    return locate()


# -- synthetic source generation -----------------------------------------------


def _lavfi_video_clip(
    tmp_path: Path,
    ffmpeg_binaries: FfmpegBinaries,
    *,
    name: str,
    size: str,
    seconds: float,
    pattern: str = "testsrc2",
) -> Path:
    """A deterministic synthetic clip, generated fresh rather than committed -
    never commit a video file to the repo, per this task's brief. Generated
    already at the target canvas size, standing in for what Task 9's
    ``normalise_clip`` would have produced from some real source.

    ``pattern`` defaults to ``testsrc2`` but is overridable: ``testsrc2`` is a
    fully deterministic generator with no seed, so two clips built from it
    with identical ``size``/``rate``/``seconds`` are byte-identical and the
    content-addressed store correctly deduplicates them to one digest - which
    would silently turn a test meaning to exercise "two distinct clips, one
    reused" into three references to the same file by accident. Callers that
    need genuinely distinct content pass a different lavfi source name
    (``smptebars``, also built into every ffmpeg).
    """
    out = tmp_path / f"{name}.mp4"
    result = subprocess.run(
        [
            str(ffmpeg_binaries.ffmpeg),
            "-hide_banner",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"{pattern}=size={size}:rate=30",
            "-t",
            str(seconds),
            "-an",
            "-pix_fmt",
            "yuv420p",
            "-c:v",
            "libx264",
            str(out),
        ],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return out


def _lavfi_narration(tmp_path: Path, ffmpeg_binaries: FfmpegBinaries, *, seconds: float) -> Path:
    """A deterministic synthetic narration track - a sine tone standing in
    for real TTS output. Task 5's ``narration.mp3`` is just an mp3 file; the
    stage under test never inspects its content, only its duration and the
    presence of an audio stream."""
    out = tmp_path / "narration.mp3"
    result = subprocess.run(
        [
            str(ffmpeg_binaries.ffmpeg),
            "-hide_banner",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency=440:duration={seconds}",
            "-c:a",
            "libmp3lame",
            str(out),
        ],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return out


def probe_duration(path: Path, *, ffprobe: Path) -> float:
    """ffprobe's own container-level duration. Local to this test file: no
    production caller needs it yet, so it does not belong in
    ``infra.ffmpeg.media_probe`` on the strength of one test alone."""
    result = subprocess.run(
        [str(ffprobe), "-v", "error", "-print_format", "json", "-show_format", str(path)],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    payload: dict[str, Any] = json.loads(result.stdout)
    return float(payload["format"]["duration"])


def probe_has_audio(path: Path, *, ffprobe: Path) -> bool:
    """Whether ``path`` carries at least one audio stream."""
    result = subprocess.run(
        [str(ffprobe), "-v", "error", "-print_format", "json", "-show_streams", str(path)],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    payload: dict[str, Any] = json.loads(result.stdout)
    streams: list[dict[str, Any]] = payload.get("streams", [])
    return any(s.get("codec_type") == "audio" for s in streams)


# -- the environment builder ----------------------------------------------------


def _timeline_json(duration_s: float) -> bytes:
    """A minimal but real ``timeline.json`` shape: one caption group of two
    words, matching ``json.dumps(asdict(timeline))``'s
    ``{"duration_s", "groups": [{"start_s", "end_s", "words": [[text, start_s,
    end_s], ...]}], "segments": [...]}`` layout (confirmed against
    ``core.pipeline.timeline.Timeline``/``CaptionGroup`` before writing this).
    ``segments`` is included for fidelity even though ``ComposeStage`` never
    reads it (that comes from ``segments.json`` instead)."""
    payload = {
        "duration_s": duration_s,
        "groups": [
            {
                "start_s": 0.2,
                "end_s": 1.4,
                "words": [["testing", 0.2, 0.8], ["captions", 0.8, 1.4]],
            }
        ],
        "segments": [{"start_s": 0.0, "end_s": duration_s}],
    }
    return json.dumps(payload).encode("utf-8")


def _segments_json(clip_ids: list[str], *, segment_seconds: float) -> bytes:
    """A real ``segments.json`` shape: ``{"clip_id", "in_point_s",
    "duration_s"}`` per entry, per ``select_broll.py``'s own documented
    output contract - never a digest, so this same array would serve
    ``compose_vertical`` (Task 12) unchanged."""
    payload = [
        {"clip_id": clip_id, "in_point_s": 0.0, "duration_s": segment_seconds}
        for clip_id in clip_ids
    ]
    return json.dumps(payload).encode("utf-8")


def _manifest_json(entries: dict[str, str]) -> bytes:
    """A real manifest shape (``infra.broll.BrollLibrary.write_manifest``):
    one object per clip with both canvas digests. Only
    ``normalised_landscape_digest`` is exercised by ``compose_landscape``;
    ``normalised_vertical_digest`` is filled with the same digest since
    nothing under test reads it."""
    payload = [
        {
            "clip_id": clip_id,
            "duration_s": 5.0,
            "source_width": 1920,
            "source_height": 1080,
            "normalised_landscape_digest": digest,
            "normalised_vertical_digest": digest,
        }
        for clip_id, digest in entries.items()
    ]
    return json.dumps(payload).encode("utf-8")


def test_a_landscape_master_is_rendered_with_burned_captions(
    tmp_path: Path, cas: CasStore, ffmpeg_binaries: FfmpegBinaries
) -> None:
    """Three B-roll segments (two distinct clips, one reused) concatenate,
    burn captions, and mux the narration in a single ffmpeg pass."""
    narration_seconds = 3.0

    clip_a = _lavfi_video_clip(
        tmp_path, ffmpeg_binaries, name="clip_a", size="1920x1080", seconds=5.0, pattern="testsrc2"
    )
    clip_b = _lavfi_video_clip(
        tmp_path, ffmpeg_binaries, name="clip_b", size="1920x1080", seconds=5.0, pattern="smptebars"
    )
    narration = _lavfi_narration(tmp_path, ffmpeg_binaries, seconds=narration_seconds)

    clip_a_digest = cas.put_file(clip_a, kind="video")
    clip_b_digest = cas.put_file(clip_b, kind="video")
    assert clip_a_digest != clip_b_digest, (
        "the two source patterns must produce genuinely distinct content, or "
        "this test's 'two distinct clips, one reused' framing is not real"
    )
    manifest_digest = cas.put_bytes(
        _manifest_json({"clip-a": clip_a_digest, "clip-b": clip_b_digest}),
        kind="broll_manifest",
    )

    # Three segments, one second each, covering the narration exactly - "a" is
    # reused for the third segment, exercising the same clip appearing twice
    # in one concat graph.
    segments_digest = cas.put_bytes(
        _segments_json(["clip-a", "clip-b", "clip-a"], segment_seconds=1.0), kind="json"
    )
    timeline_digest = cas.put_bytes(_timeline_json(narration_seconds), kind="json")
    narration_digest = cas.put_file(narration, kind="audio")

    ctx = JobContext(
        job_id="it-compose",
        project_id="it-project",
        settings={
            "broll_manifest_digest": str(manifest_digest),
            "caption_style": {},
            "encoder": "auto",
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

    stage = make_compose_landscape(cas=cas, settings={})
    result = stage.run(ctx, lambda fraction, note: None)

    out = cas.path_for(result.artifact("master_1920x1080.mp4").digest)
    assert probe_dimensions(out, ffprobe=ffmpeg_binaries.ffprobe) == (1920, 1080)
    assert probe_duration(out, ffprobe=ffmpeg_binaries.ffprobe) == pytest.approx(
        narration_seconds, abs=0.3
    )
    assert probe_has_audio(out, ffprobe=ffmpeg_binaries.ffprobe)

    # The .ass side artifact: proves font_size was actually derived from this
    # canvas's own height (1080 // 20 == 54), not left to render_ass's
    # canvas-agnostic default of 96 - this task's first bound decision.
    ass_text = cas.read_bytes(result.artifact("captions.ass").digest).decode("utf-8")
    assert "PlayResX: 1920" in ass_text
    assert "PlayResY: 1080" in ass_text
    assert "Style: Default,Arial,54," in ass_text
    assert "Dialogue:" in ass_text


def test_a_missing_clip_id_in_the_manifest_is_a_fatal_provider_error(
    tmp_path: Path, cas: CasStore, ffmpeg_binaries: FfmpegBinaries
) -> None:
    """A segments.json that outran the manifest (a clip removed from the
    library after select_broll ran) must fail loudly and name the clip,
    never silently skip a segment or crash with a bare KeyError."""
    narration = _lavfi_narration(tmp_path, ffmpeg_binaries, seconds=1.0)
    manifest_digest = cas.put_bytes(_manifest_json({}), kind="broll_manifest")
    segments_digest = cas.put_bytes(
        _segments_json(["missing-clip"], segment_seconds=1.0), kind="json"
    )
    timeline_digest = cas.put_bytes(_timeline_json(1.0), kind="json")
    narration_digest = cas.put_file(narration, kind="audio")

    ctx = JobContext(
        job_id="it-compose-missing",
        project_id="it-project",
        settings={
            "broll_manifest_digest": str(manifest_digest),
            "caption_style": {},
            "encoder": "auto",
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

    stage = make_compose_landscape(cas=cas, settings={})
    with pytest.raises(ProviderError) as exc_info:
        stage.run(ctx, lambda fraction, note: None)

    assert exc_info.value.kind is ErrorKind.FATAL
    assert "missing-clip" in str(exc_info.value)
