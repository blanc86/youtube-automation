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

from ytauto.app.stages.compose import make_compose_landscape, make_compose_vertical
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import hash_bytes
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


def test_the_vertical_master_uses_the_vertical_normalised_clips(
    tmp_path: Path, cas: CasStore, ffmpeg_binaries: FfmpegBinaries
) -> None:
    """Resolving ``clip_id`` here would still render at 1080x1920 even if the
    stage read ``normalised_landscape_digest`` instead: ``compose_args``
    always applies its own defensive scale/pad chain to whatever source path
    it is given (``infra.ffmpeg.compose.ComposeClip``'s own docstring), so a
    landscape source would come out letterboxed rather than fail. A dimension
    assertion alone cannot tell the two apart.

    This test pins the manifest field directly instead: the manifest's
    ``normalised_landscape_digest`` entry is a well-formed digest that was
    never staged into the CAS (``hash_bytes`` of some bytes nobody wrote),
    while ``normalised_vertical_digest`` points at a real clip. A stage that
    (bug) resolved against the landscape field would hand ffmpeg a path to a
    file that does not exist and fail with a ``ProviderError``; a stage that
    correctly resolves ``normalised_vertical_digest`` renders normally.
    """
    narration_seconds = 1.0
    clip = _lavfi_video_clip(
        tmp_path, ffmpeg_binaries, name="clip_vertical", size="1080x1920", seconds=2.0
    )
    narration = _lavfi_narration(tmp_path, ffmpeg_binaries, seconds=narration_seconds)

    vertical_digest = cas.put_file(clip, kind="video")
    bogus_landscape_digest = hash_bytes(b"never staged - compose_vertical must not read this")

    manifest_digest = cas.put_bytes(
        json.dumps(
            [
                {
                    "clip_id": "clip-a",
                    "duration_s": 2.0,
                    "source_width": 1080,
                    "source_height": 1920,
                    "normalised_landscape_digest": str(bogus_landscape_digest),
                    "normalised_vertical_digest": str(vertical_digest),
                }
            ]
        ).encode("utf-8"),
        kind="broll_manifest",
    )
    segments_digest = cas.put_bytes(
        _segments_json(["clip-a"], segment_seconds=narration_seconds), kind="json"
    )
    timeline_digest = cas.put_bytes(_timeline_json(narration_seconds), kind="json")
    narration_digest = cas.put_file(narration, kind="audio")

    ctx = JobContext(
        job_id="it-compose-vertical",
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

    stage = make_compose_vertical(cas=cas, settings={})
    result = stage.run(ctx, lambda fraction, note: None)

    out = cas.path_for(result.artifact("master_1080x1920.mp4").digest)
    assert probe_dimensions(out, ffprobe=ffmpeg_binaries.ffprobe) == (1080, 1920)


def test_both_canvases_render_from_one_select_broll_result(
    tmp_path: Path, cas: CasStore, ffmpeg_binaries: FfmpegBinaries
) -> None:
    """One manifest, one segments.json, one timeline.json, one narration -
    exactly what select_broll/plan_timeline produce once per project, never
    once per canvas - feed both compose_landscape and compose_vertical
    unchanged. The two canvases must still diverge downstream:
    ``render_ass`` writes ``width``/``height`` as ``PlayResX``/``PlayResY``
    and derives ``font_size`` from ``height`` (``_FONT_SIZE_DIVISOR``), so the
    two rendered ``.ass`` blobs must have different digests. Equal digests
    here would mean the canvas never reached the caption writer, and every
    vertical caption would be burned in at the landscape font size and
    PlayRes.
    """
    narration_seconds = 1.0
    clip = _lavfi_video_clip(
        tmp_path, ffmpeg_binaries, name="clip_shared", size="1920x1080", seconds=2.0
    )
    narration = _lavfi_narration(tmp_path, ffmpeg_binaries, seconds=narration_seconds)

    clip_digest = cas.put_file(clip, kind="video")
    manifest_digest = cas.put_bytes(_manifest_json({"clip-a": clip_digest}), kind="broll_manifest")
    segments_digest = cas.put_bytes(
        _segments_json(["clip-a"], segment_seconds=narration_seconds), kind="json"
    )
    timeline_digest = cas.put_bytes(_timeline_json(narration_seconds), kind="json")
    narration_digest = cas.put_file(narration, kind="audio")

    def _ctx(job_id: str) -> JobContext:
        return JobContext(
            job_id=job_id,
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
            # Distinct workdirs: both stages write a same-named
            # master/captions.ass pair into ctx.workdir, and running them
            # sequentially against one shared directory would let the second
            # stage's files silently overwrite the first's before either is
            # staged into the CAS.
            workdir=tmp_path / job_id,
        )

    land = make_compose_landscape(cas=cas, settings={}).run(_ctx("it-land"), lambda f, n: None)
    vert = make_compose_vertical(cas=cas, settings={}).run(_ctx("it-vert"), lambda f, n: None)

    # Digest inequality alone is not a sufficient pin: font_size is also
    # derived from height (_FONT_SIZE_DIVISOR) independently of the
    # width/height render_ass is actually called with, so a bug that hands
    # render_ass the wrong canvas but leaves font_size alone would still
    # produce two different digests here and hide behind this assertion
    # alone (confirmed by deliberately hardcoding compose.py's render_ass
    # call to width=1920, height=1080 for both stages while running this
    # test suite - the digest-only assertion below still passed). The
    # PlayResX/PlayResY checks are what actually pin "each canvas reached
    # the caption writer with its own dimensions".
    land_ass = cas.read_bytes(land.artifact("captions.ass").digest).decode("utf-8")
    vert_ass = cas.read_bytes(vert.artifact("captions.ass").digest).decode("utf-8")
    assert "PlayResX: 1920" in land_ass
    assert "PlayResY: 1080" in land_ass
    assert "PlayResX: 1080" in vert_ass
    assert "PlayResY: 1920" in vert_ass

    assert land.artifact("captions.ass").digest != vert.artifact("captions.ass").digest, (
        "each canvas needs its own PlayResX/Y"
    )


# -- the music bed, measured in the rendered file -------------------------------


def _lavfi_tone(
    tmp_path: Path,
    ffmpeg_binaries: FfmpegBinaries,
    *,
    name: str,
    seconds: float,
    frequency: int = 220,
    volume: str = "1.0",
) -> Path:
    """A synthetic audio file. ``volume=0`` gives true digital silence, which
    is what makes the bed's own level measurable in the mixed output."""
    out = tmp_path / f"{name}.mp3"
    result = subprocess.run(
        [
            str(ffmpeg_binaries.ffmpeg),
            "-hide_banner",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency={frequency}:duration={seconds}",
            "-af",
            f"volume={volume}",
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


def _mean_volume_db(
    path: Path, *, ffmpeg: Path, start_s: float | None = None, duration_s: float | None = None
) -> float:
    """The mean volume ffmpeg's own ``volumedetect`` reports, in dBFS.

    Optionally over a window, which is how the loop test asks whether music is
    still playing after the track's own length has already elapsed.
    """
    args = [str(ffmpeg), "-hide_banner"]
    if start_s is not None:
        args += ["-ss", str(start_s)]
    if duration_s is not None:
        args += ["-t", str(duration_s)]
    args += ["-i", str(path), "-af", "volumedetect", "-f", "null", "-"]
    result = subprocess.run(
        args, capture_output=True, encoding="utf-8", errors="replace", timeout=120, check=False
    )
    assert result.returncode == 0, result.stderr
    for line in result.stderr.splitlines():
        if "mean_volume:" in line:
            return float(line.split("mean_volume:")[1].strip().split()[0])
    raise AssertionError(f"volumedetect reported no mean_volume for {path}")


def _compose_with(
    tmp_path: Path,
    cas: CasStore,
    ffmpeg_binaries: FfmpegBinaries,
    *,
    job_id: str,
    narration: Path,
    music: Path | None,
    gain_db: float = 0.0,
    video_seconds: float = 3.0,
) -> Path:
    """Render one landscape master over a single clip, with or without a bed.

    Returns the path of the rendered file in the CAS.
    """
    clip = _lavfi_video_clip(
        tmp_path,
        ffmpeg_binaries,
        name=f"clip_{job_id}",
        size="1920x1080",
        seconds=video_seconds + 2.0,
        pattern="testsrc2",
    )
    manifest_digest = cas.put_bytes(
        _manifest_json({"clip-a": cas.put_file(clip, kind="video")}), kind="broll_manifest"
    )
    segments_digest = cas.put_bytes(
        _segments_json(["clip-a"], segment_seconds=video_seconds), kind="json"
    )
    settings: dict[str, object] = {
        "broll_manifest_digest": str(manifest_digest),
        "caption_style": {},
        "encoder": "auto",
        "music_digest": "",
        "music_gain_db": gain_db,
    }
    if music is not None:
        settings["music_digest"] = str(cas.put_file(music, kind="audio"))

    ctx = JobContext(
        job_id=job_id,
        project_id="it-music",
        settings=settings,
        inputs={
            "plan_timeline": (
                ArtifactRef(
                    name="timeline.json",
                    kind="json",
                    digest=cas.put_bytes(_timeline_json(video_seconds), kind="json"),
                ),
            ),
            "select_broll": (
                ArtifactRef(name="segments.json", kind="json", digest=segments_digest),
            ),
            "synthesize_speech": (
                ArtifactRef(
                    name="narration.mp3", kind="audio", digest=cas.put_file(narration, kind="audio")
                ),
            ),
        },
        workdir=tmp_path / f"work-{job_id}",
    )
    result = make_compose_landscape(cas=cas, settings={}).run(ctx, lambda fraction, note: None)
    return cas.path_for(result.artifact("master_1920x1080.mp4").digest)


def test_a_music_bed_is_audible_in_the_rendered_master(
    tmp_path: Path, cas: CasStore, ffmpeg_binaries: FfmpegBinaries
) -> None:
    """The end-to-end claim, measured rather than assumed: with a silent
    narration the only thing that can make noise in the output is the bed, so
    a rendered file that is still silent means the music never arrived.

    Silence is the control precisely because the caption bug proved that "the
    code changed" and "the file changed" are different statements.
    """
    silence = _lavfi_tone(tmp_path, ffmpeg_binaries, name="silent", seconds=3.0, volume="0")
    bed = _lavfi_tone(tmp_path, ffmpeg_binaries, name="bed", seconds=4.0, frequency=220)

    without = _compose_with(
        tmp_path, cas, ffmpeg_binaries, job_id="no-bed", narration=silence, music=None
    )
    with_bed = _compose_with(
        tmp_path, cas, ffmpeg_binaries, job_id="bed", narration=silence, music=bed, gain_db=0.0
    )

    quiet = _mean_volume_db(without, ffmpeg=ffmpeg_binaries.ffmpeg)
    loud = _mean_volume_db(with_bed, ffmpeg=ffmpeg_binaries.ffmpeg)

    assert quiet < -60.0, f"the control render should be silent, measured {quiet} dBFS"
    assert loud > quiet + 30.0, (
        f"the bed is not in the rendered file: silent render {quiet} dBFS, "
        f"render with music {loud} dBFS"
    )


def test_the_gain_setting_changes_how_loud_the_bed_actually_is(
    tmp_path: Path, cas: CasStore, ffmpeg_binaries: FfmpegBinaries
) -> None:
    """Independent volume has to mean something in the file, not just in the
    filter string."""
    silence = _lavfi_tone(tmp_path, ffmpeg_binaries, name="silent2", seconds=3.0, volume="0")
    bed = _lavfi_tone(tmp_path, ffmpeg_binaries, name="bed2", seconds=4.0, frequency=220)

    full = _compose_with(
        tmp_path, cas, ffmpeg_binaries, job_id="g0", narration=silence, music=bed, gain_db=0.0
    )
    quietened = _compose_with(
        tmp_path, cas, ffmpeg_binaries, job_id="g20", narration=silence, music=bed, gain_db=-20.0
    )

    loud = _mean_volume_db(full, ffmpeg=ffmpeg_binaries.ffmpeg)
    soft = _mean_volume_db(quietened, ffmpeg=ffmpeg_binaries.ffmpeg)

    # 20 dB asked for; allow generous slack for the mp3/aac round trips and the
    # tail fade, but the direction and rough magnitude must be real.
    assert soft < loud - 12.0, f"-20 dB barely changed anything: {loud} -> {soft} dBFS"


def test_a_track_shorter_than_the_video_loops_instead_of_falling_silent(
    tmp_path: Path, cas: CasStore, ffmpeg_binaries: FfmpegBinaries
) -> None:
    """A 30-second bed under a three-minute video is the ordinary case. Without
    -stream_loop the audio simply stops partway through and nothing complains.

    Measured in a window that begins after the track's own length has already
    elapsed, and ends before the tail fade starts.
    """
    silence = _lavfi_tone(tmp_path, ffmpeg_binaries, name="silent3", seconds=3.0, volume="0")
    short_bed = _lavfi_tone(tmp_path, ffmpeg_binaries, name="short", seconds=0.5, frequency=220)

    out = _compose_with(
        tmp_path,
        cas,
        ffmpeg_binaries,
        job_id="loop",
        narration=silence,
        music=short_bed,
        gain_db=0.0,
        video_seconds=3.0,
    )

    # The bed is 0.5s; the fade starts at 3.0 - 1.5 = 1.5s. This window is
    # entirely past the end of one pass and entirely before the fade.
    later = _mean_volume_db(out, ffmpeg=ffmpeg_binaries.ffmpeg, start_s=0.7, duration_s=0.7)
    assert later > -60.0, (
        f"the bed stopped after one pass instead of looping: {later} dBFS at 0.7-1.4s"
    )


def test_two_tracks_at_wildly_different_levels_land_at_the_same_bed_level(
    tmp_path: Path, cas: CasStore, ffmpeg_binaries: FfmpegBinaries
) -> None:
    """This is the bug report, reduced to a measurement.

    ``music_gain_db`` used to attenuate a track whose own level was unknown.
    Two perfectly legitimate files 24 dB apart - a quietly mastered bed and a
    loud one - therefore produced an inaudible bed and a reasonable one from
    the identical setting, and the honest description of that is "background
    music is not working".

    Normalising before the trim is what makes the control mean something, so
    the assertion is that the *rendered masters* agree, not that the filter
    string contains a particular filter name.
    """
    silence = _lavfi_tone(tmp_path, ffmpeg_binaries, name="sil-n", seconds=3.0, volume="0")
    quiet = _lavfi_tone(
        tmp_path, ffmpeg_binaries, name="quiet", seconds=4.0, frequency=220, volume="0.05"
    )
    loud = _lavfi_tone(
        tmp_path, ffmpeg_binaries, name="loud", seconds=4.0, frequency=220, volume="1.0"
    )

    raw_gap = abs(
        _mean_volume_db(quiet, ffmpeg=ffmpeg_binaries.ffmpeg)
        - _mean_volume_db(loud, ffmpeg=ffmpeg_binaries.ffmpeg)
    )
    assert raw_gap > 15.0, (
        f"the two source tracks must genuinely differ for this test to mean "
        f"anything; they differ by {raw_gap:.1f} dB"
    )

    quiet_master = _compose_with(
        tmp_path, cas, ffmpeg_binaries, job_id="q", narration=silence, music=quiet, gain_db=0.0
    )
    loud_master = _compose_with(
        tmp_path, cas, ffmpeg_binaries, job_id="l", narration=silence, music=loud, gain_db=0.0
    )

    rendered_gap = abs(
        _mean_volume_db(quiet_master, ffmpeg=ffmpeg_binaries.ffmpeg)
        - _mean_volume_db(loud_master, ffmpeg=ffmpeg_binaries.ffmpeg)
    )
    assert rendered_gap < 4.0, (
        f"the same setting produced beds {rendered_gap:.1f} dB apart from sources "
        f"{raw_gap:.1f} dB apart - the bed is not being normalised"
    )
