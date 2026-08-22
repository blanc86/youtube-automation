"""Task 14: Phase 2a's four exit criteria (spec Sec 1.3), proven end to end
through the real CLI - ``ytauto broll add`` / ``ytauto project create`` /
``ytauto run`` - against a real ``story_video`` pipeline: real edge-tts
network synthesis, real ffmpeg encodes, real B-roll (generated with
``ffmpeg -f lavfi``, never committed to the repo).

**Criterion 2's wording versus what is actually provable.** The spec says
"re-running the same job spawns zero workers", but ``ytauto run`` always
enqueues a *fresh* job (``cli/__main__.py``'s ``_run`` mints a new
``uuid.uuid4().hex`` on every invocation) - there is no CLI verb that resumes
one specific job id by choice. ``test_rerunning_the_same_job_spawns_no_workers``
below tests the reading that is actually true and, arguably, the stronger
claim: a *second, independent* job against the same project, with unchanged
settings and unchanged upstream state, hits the fingerprint cache on every
one of the seven stages. That is "identical fingerprints produce all cache
hits on a new job", not literally "the same job runs twice". Recorded here
rather than silently reworded into the criterion's own docstring.

**edge-tts and the network.** ``synthesize_speech`` calls the real,
unofficial Microsoft edge-tts endpoint (spec Sec 12's own risk register:
"can break without notice"). The decision here is to run it for real, on
every criterion, rather than fake it: faking the synthesizer would prove the
wiring but not the thing this task exists to prove ("it works"), and the
caching design this suite is built to pin already minimises real network
calls on its own - a cache hit never re-invokes the synthesizer, so most
criteria call it once, `test_changing_the_voice_does_not_rerun_ingest_story`
twice. What is added is a diagnostic seam: ``_require_edge_tts_reachable``
(module-scoped, autouse) runs a short, bounded (20s, via a subprocess so a
truly hung connection is *killed*, not merely abandoned) real synthesis
before any criterion runs, and skips the whole module with a message that
names it a network problem, not a pipeline defect, if it fails. Without this,
an offline run would fail all five criteria with the same generic FATAL
ProviderError, indistinguishable from a real regression.

**The kill/resume mechanism (criterion 3).** ``tests/integration/test_resume.py``
already proves the resume mechanism against a synthetic three-stage pipeline
with a marker-file handshake baked into each stage specifically so a test can
pause it. The real ``story_video`` stages have no such hook, so
``FirstLightEnv.run_until_stage``/``kill_running_worker`` adapt the same
established pattern (a background thread drives the blocking
``Dispatcher.tick()`` call; the foreground thread never touches the shared
sqlite connection, only the in-memory ``Dispatcher._running`` dict and the
``Popen`` handle it finds there - dispatcher.py's own "one connection per
thread" rule) but poll ``_running`` directly instead of waiting on a marker,
since there is none to wait on.

Killing only the immediate worker process (``Popen.kill()`` alone) would
leave a real risk unaddressed: ``compose_landscape`` shells out to ffmpeg as
its own subprocess, and ``ctx.workdir`` is job+stage scoped, not
attempt-numbered, so a resumed attempt's ffmpeg would write to the exact same
output path a killed attempt's *orphaned* ffmpeg might still be writing to -
which is precisely the "two ffmpeg processes, one output file" question a
prior attempt at this task stalled trying to answer empirically. This avoids
answering it rather than answering it: ``_kill_process_tree`` uses
``taskkill /T /F`` (Windows; this project's own platform) to terminate the
whole process tree the worker spawned, so no orphaned ffmpeg can ever exist
for a resumed attempt to race against.

**Spawn accounting.** Neither ``job_stages.status`` nor ``job_stages.attempts``
alone counts "how many times was a worker launched for this stage" - status
only ever reaches ``succeeded`` on completion (a killed attempt never gets
there) and ``attempts`` is bumped only on the retry path (never on success -
confirmed by reading ``dispatcher.py``'s ``_bump_stage_attempts`` call sites).
So ``FirstLightEnv`` monkeypatches ``Dispatcher.tick`` for the lifetime of one
test (via the ``monkeypatch`` fixture, auto-reverted) to record every
``TickReport.spawned``/``.skipped`` as it happens, regardless of which
Dispatcher instance produced it (the CLI's own internal one, or the harness's
low-level one for ``run_until_stage``) or whether that spawn ever completed.
"""

from __future__ import annotations

import contextlib
import functools
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from ytauto.app.registry import build_pipeline
from ytauto.app.scheduler import dispatcher as dispatcher_module
from ytauto.app.scheduler.dispatcher import Dispatcher, TickReport
from ytauto.app.scheduler.governor import Governor
from ytauto.app.scheduler.queue import JobQueue
from ytauto.app.services.enqueue import resolve_project_id
from ytauto.app.services.projects import ProjectService
from ytauto.cli.__main__ import main
from ytauto.core.models.artifact import ArtifactRef
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.broll import BrollLibrary
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations
from ytauto.infra.ffmpeg.locator import FfmpegBinaries, locate
from ytauto.infra.ffmpeg.media_probe import probe_dimensions as _probe_dimensions_impl
from ytauto.infra.paths import AppPaths

pytestmark = pytest.mark.integration

_PIPELINE_ID = "story_video"

_STAGE_ORDER: tuple[str, ...] = (
    "ingest_story",
    "synthesize_speech",
    "transcribe",
    "plan_timeline",
    "select_broll",
    "compose_landscape",
    "compose_vertical",
)
"""The pipeline's real topology (spec Sec 3): a strict chain up to
``select_broll``, then the one antichain - ``compose_landscape`` and
``compose_vertical`` both depend on ``{synthesize_speech, plan_timeline,
select_broll}``, not on each other. ``Pipeline.ready_stages`` sorts ties by
stage id (``core/pipeline/graph.py``), so ``compose_landscape`` <
``compose_vertical`` alphabetically is what makes the dispatcher pick
landscape first once both are ready - confirmed by reading that method
before relying on it."""

_VOICE = "en-US-AriaNeural"
_ALT_VOICE = "en-GB-RyanNeural"
"""Both confirmed real edge-tts voices via a live ``edge_tts.list_voices()``
call before writing this file - a malformed voice name would fail FATAL, not
RETRYABLE, on the very first synthesis and be easy to mistake for a broken
pipeline."""

_DEFAULT_SETTINGS: dict[str, object] = {
    "voice": _VOICE,
    "rate": "+0%",
    "seed": 1,
    "words_per_group_min": 3,
    "words_per_group_max": 5,
    "segment_seconds_min": 1.5,
    "segment_seconds_max": 4.0,
    "caption_style": {},
    "encoder": "auto",
}
"""Every settings key some stage's ``settings_keys`` declares, other than
``story_digest``/``story_path`` (set by ``ytauto project create`` itself) and
``broll_manifest_digest`` (set once the B-roll library exists - see
``first_light_env``). Nothing in the CLI seeds these today - there is no
``ytauto project set-setting`` verb yet - so a test harness driving the real
pipeline has to seed them directly via ``ProjectService.set_setting``, the
same primitive a future CLI verb would itself call."""

_BROLL_PATTERNS: tuple[str, ...] = (
    "testsrc2",
    "smptebars",
    "yuvtestsrc",
    "rgbtestsrc",
    "pal75bars",
    "pal100bars",
    "colorspectrum",
)
"""Distinct ``-f lavfi`` source filters (all built into ffmpeg, confirmed
present in this build via ``ffmpeg -filters`` before writing this), so
``clips`` B-roll sources are genuinely distinct content, not
``CasStore``-deduplicated copies of one pattern - mirrors
``tests/integration/test_compose.py``'s own note on why ``testsrc2`` alone
cannot make two distinct clips."""

_BROLL_CLIP_SECONDS = 8.0
"""Comfortably longer than ``_DEFAULT_SETTINGS["segment_seconds_max"]`` (4.0)
so ``LibraryVisualStrategy.plan``'s duration filter never has to reject the
one clip a short segment needs - these stories are one or two sentences, so
plan_timeline can only ever produce a handful of short segments."""

_BROLL_SIZE = "1280x720"
"""The *source* clip's own resolution - unrelated to either output canvas.
``BrollLibrary.add`` always transcodes to both 1920x1080 and 1080x1920 via
scale-and-pad regardless of the source's own size, so a small source keeps
ingest fast without affecting correctness."""


# -- ffmpeg/ffprobe helpers -------------------------------------------------


@functools.lru_cache(maxsize=1)
def _binaries() -> FfmpegBinaries:
    """Located once per test process - ``locate()`` shells out for a version
    string, and every criterion in this module needs the same pair."""
    return locate()


def probe_dimensions(path: Path) -> tuple[int, int]:
    return _probe_dimensions_impl(path, ffprobe=_binaries().ffprobe)


def probe_duration(path: Path) -> float:
    """A media file's own container-level duration via ffprobe. Mirrors
    ``tests/integration/test_compose.py``'s identical helper - not promoted to
    ``infra.ffmpeg.media_probe`` on the strength of two test files alone."""
    ffprobe = str(_binaries().ffprobe)
    result = subprocess.run(
        [ffprobe, "-v", "error", "-print_format", "json", "-show_format", str(path)],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    payload: dict[str, Any] = json.loads(result.stdout)
    return float(payload["format"]["duration"])


def probe_has_audio(path: Path) -> bool:
    ffprobe = str(_binaries().ffprobe)
    result = subprocess.run(
        [ffprobe, "-v", "error", "-print_format", "json", "-show_streams", str(path)],
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


def _make_broll_clip(tmp_path: Path, ffmpeg: Path, *, index: int) -> Path:
    """A deterministic, synthetic B-roll source clip - never committed to the
    repo, generated fresh via ``-f lavfi`` per this task's brief. ``-y`` is
    passed explicitly (a prior attempt at this task stalled for ten minutes on
    an ad-hoc ffmpeg command that omitted it and blocked on an
    ``Overwrite? [y/N]`` prompt with no stdin attached) and the call is
    timeout-bounded."""
    pattern = _BROLL_PATTERNS[index % len(_BROLL_PATTERNS)]
    out = tmp_path / f"broll_source_{index}.mp4"
    result = subprocess.run(
        [
            str(ffmpeg),
            "-hide_banner",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"{pattern}=size={_BROLL_SIZE}:rate=30",
            "-t",
            str(_BROLL_CLIP_SECONDS),
            "-an",
            "-pix_fmt",
            "yuv420p",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
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


# -- edge-tts reachability preflight -----------------------------------------

_PREFLIGHT_SCRIPT = f"""
import asyncio, sys
import edge_tts

async def main():
    c = edge_tts.Communicate(
        "connectivity check", {_VOICE!r}, rate="+0%", boundary="WordBoundary"
    )
    async for chunk in c.stream():
        if chunk["type"] == "audio":
            sys.exit(0)
    sys.exit(1)

asyncio.run(main())
"""


@pytest.fixture(scope="module", autouse=True)
def _require_edge_tts_reachable() -> None:
    """Skip this whole module, with a clear reason, if edge-tts is not
    reachable - rather than let all five criteria fail identically through
    five retries and exponential backoff apiece (~150s each) with no way to
    tell "the network is down" apart from "the pipeline is broken".

    Run as a **subprocess** with a hard ``timeout=``, not a background thread:
    ``asyncio.run`` inside ``EdgeTtsSynthesizer.synthesize`` has no timeout of
    its own, and a Python thread blocked in it cannot be forcibly killed if
    the connection hangs rather than fails - it would leak past this fixture
    and could block interpreter shutdown. ``subprocess.run(..., timeout=)``
    guarantees the child is killed either way - but only if its own
    ``TimeoutExpired`` is caught here rather than left to propagate: a
    firewall that silently black-holes packets (no RST, the common
    corporate-network case) hits exactly the timeout path, not a nonzero
    exit, and an uncaught ``TimeoutExpired`` would ERROR every test in this
    module instead of SKIPPING it with the diagnosis this fixture exists to
    give.
    """
    try:
        result = subprocess.run(
            [sys.executable, "-c", _PREFLIGHT_SCRIPT],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
            check=False,
        )
    except subprocess.TimeoutExpired:
        pytest.skip(
            "edge-tts is not reachable from this environment - this is a network "
            "problem, not a pipeline defect (preflight did not respond within 20s, "
            "consistent with a firewall silently dropping the connection rather than "
            "refusing it). See this module's docstring."
        )
    if result.returncode != 0:
        pytest.skip(
            "edge-tts is not reachable from this environment - this is a network "
            f"problem, not a pipeline defect (preflight exit {result.returncode}; "
            f"stderr: {result.stderr.strip()[-500:]!r}). See this module's "
            "docstring."
        )


# -- process-tree kill (see module docstring) --------------------------------


def _kill_process_tree(pid: int) -> None:
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
            check=False,
        )
    else:  # pragma: no cover - this project's platform is win32
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(os.getpgid(pid), signal.SIGKILL)


# -- db/paths helpers ---------------------------------------------------------


def _open(paths: AppPaths) -> sqlite3.Connection:
    conn = connect(paths.db_file)
    apply_migrations(conn)
    return conn


# -- the report a CLI-level run produced --------------------------------------


@dataclass(frozen=True)
class RunReport:
    """What one ``ytauto run`` invocation did - reconstructed from
    ``Dispatcher.tick`` reports captured while it ran, since ``main()`` itself
    returns only an exit code. See the module docstring's "spawn accounting"
    section for why this cannot be derived from ``job_stages`` after the
    fact."""

    spawned: tuple[str, ...]
    skipped: tuple[str, ...]


# -- the environment ----------------------------------------------------------


class FirstLightEnv:
    """One project: a real story, a real B-roll library, real settings -
    created through ``ytauto broll add`` / ``ytauto project create``, driven
    through ``ytauto run``. ``run_until_stage``/``kill_running_worker``/
    ``fast_forward_backoff`` are the one exception (see the module docstring)
    and drive a ``Dispatcher`` directly, exactly as
    ``tests/integration/test_resume.py`` does.
    """

    _ARTIFACT_STAGE: dict[str, str] = {
        "master_1920x1080.mp4": "compose_landscape",
        "master_1080x1920.mp4": "compose_vertical",
    }

    def __init__(self, paths: AppPaths, slug: str, project_id: str) -> None:
        self.paths = paths
        self.slug = slug
        self.project_id = project_id
        self.spawns: list[str] = []
        self.skips: list[str] = []
        self._resume_conn: sqlite3.Connection | None = None
        self._resume_queue: JobQueue | None = None
        self._resume_dispatcher: Dispatcher | None = None
        self._resume_driver: threading.Thread | None = None
        self._resume_job_id: str | None = None
        self._resume_target_stage: str | None = None

    # -- CLI-level runs ------------------------------------------------------

    def _cli_run(self) -> int:
        # --output-dir pins the export location under the same tmp-rooted
        # AppPaths this whole environment already lives under. Left to
        # auto-detect (infra.paths.resolve_output_dir), every CLI-level run
        # in this module would create <real Videos>/ytauto (or Downloads) on
        # whatever machine runs the suite - a real side effect a hermetic
        # integration test must not have, and irrelevant to what this module
        # exists to prove (the pipeline, not where the resolver looks).
        return main(
            [
                "--data-dir",
                str(self.paths.root),
                "run",
                "--project",
                self.slug,
                "--output-dir",
                str(self.paths.root / "output"),
            ]
        )

    def run(self) -> int:
        """Invoke ``ytauto run --project <slug>`` once. Always enqueues a
        fresh job (see the module docstring's note on criterion 2's wording).
        """
        rc = self._cli_run()
        assert rc == 0, (
            f"ytauto run exited {rc} for project {self.slug!r}; see "
            f"{self.paths.logs / 'ytauto.jsonl'}"
        )
        return rc

    def run_again(self) -> RunReport:
        """Invoke ``ytauto run`` a second (or later) time and report exactly
        what that one invocation spawned/skipped - not the cumulative total
        across the whole environment's lifetime (see ``spawn_count`` for
        that)."""
        before_spawn, before_skip = len(self.spawns), len(self.skips)
        rc = self._cli_run()
        assert rc == 0, (
            f"ytauto run exited {rc} for project {self.slug!r}; see "
            f"{self.paths.logs / 'ytauto.jsonl'}"
        )
        return RunReport(
            spawned=tuple(self.spawns[before_spawn:]),
            skipped=tuple(self.skips[before_skip:]),
        )

    def spawn_count(self, stage_id: str) -> int:
        """Total spawn events for ``stage_id`` across this environment's
        entire lifetime - every job, every attempt, whether it completed,
        was killed, or errored. See the module docstring."""
        return self.spawns.count(stage_id)

    def resumed_job_stage_status(self, stage_id: str) -> str | None:
        """``job_stages.status`` for ``stage_id`` on the *specific* job
        ``run_until_stage`` enqueued and ``kill_running_worker`` killed a
        stage of - never "whichever job is latest", since ``env.run()``'s
        resume also enqueues and fully drains a brand-new job of its own
        (see criterion 3's own comment on why ``spawn_count`` alone cannot
        distinguish "resumed correctly" from "resumed via a cache hit after
        a status corruption"). Reads resume tracking directly, independent
        of whether a cache hit would also have kept the spawn count
        unchanged."""
        assert self._resume_job_id is not None, "call run_until_stage first"
        conn = _open(self.paths)
        try:
            row = conn.execute(
                "SELECT status FROM job_stages WHERE job_id = ? AND stage_id = ?",
                (self._resume_job_id, stage_id),
            ).fetchone()
            return str(row["status"]) if row is not None else None
        finally:
            conn.close()

    def set_setting(self, key: str, value: object) -> None:
        conn = _open(self.paths)
        try:
            ProjectService(conn).set_setting(self.project_id, key, value)
        finally:
            conn.close()

    # -- artifact resolution ---------------------------------------------------

    def _latest_job_id(self) -> str:
        conn = _open(self.paths)
        try:
            row = conn.execute(
                "SELECT id FROM jobs WHERE project_id = ? ORDER BY rowid DESC LIMIT 1",
                (self.project_id,),
            ).fetchone()
            assert row is not None, f"no job was ever enqueued for project {self.slug!r}"
            return str(row["id"])
        finally:
            conn.close()

    def _artifact_ref(self, job_id: str, stage_id: str, name: str) -> ArtifactRef:
        conn = _open(self.paths)
        try:
            row = conn.execute(
                "SELECT fingerprint FROM job_stages WHERE job_id = ? AND stage_id = ?",
                (job_id, stage_id),
            ).fetchone()
            assert row is not None and row["fingerprint"] is not None, (
                f"{stage_id!r} never completed for job {job_id!r}"
            )
            cas = CasStore(root=self.paths.cas, conn=conn)
            artifacts = ArtifactStore(cas, conn)
            produced = artifacts.lookup(str(row["fingerprint"]))
            assert produced is not None, f"{stage_id!r}'s artifacts are not in the cache"
            for ref in produced:
                if ref.name == name:
                    return ref
            raise AssertionError(f"{stage_id!r} never produced an artifact named {name!r}")
        finally:
            conn.close()

    def artifact_path(self, name: str) -> Path:
        """The real, on-disk, playable path for an artifact named ``name``
        from the most recently completed job."""
        job_id = self._latest_job_id()
        stage_id = self._ARTIFACT_STAGE[name]
        ref = self._artifact_ref(job_id, stage_id, name)
        conn = _open(self.paths)
        try:
            cas = CasStore(root=self.paths.cas, conn=conn)
            return cas.path_for(ref.digest)
        finally:
            conn.close()

    def artifact_digest(self, stage_id: str, name: str) -> str:
        """The content digest of one artifact ``stage_id`` produced for the
        most recently completed job - lets a test compare two runs' actual
        output content, not merely whether a stage was a cache hit."""
        job_id = self._latest_job_id()
        ref = self._artifact_ref(job_id, stage_id, name)
        return str(ref.digest)

    @property
    def narration_seconds(self) -> float:
        """The narration duration the pipeline itself computed - ``timeline.json``'s
        ``duration_s`` (``plan_timeline``, the last word's own end time), not
        ``narration.mp3``'s raw ffprobe duration.

        The two differ by however much trailing silence edge-tts appends past
        the last word - observed here as ~0.8s, comfortably past the
        ``abs=0.5`` tolerance this criterion's own pinned assertion allows.
        That gap is Task 7's accepted Phase 2a limitation (``-shortest``
        truncates the narration tail to the video's own length, which is
        built from ``timeline.duration_s``): the master's *video* track is
        deliberately built to match this value, not the raw audio file's, so
        this is the correct reference duration for "the two files' duration
        matches the narration" - comparing against the raw file would fail
        this criterion for a reason the brief explicitly rules out of scope.
        """
        job_id = self._latest_job_id()
        ref = self._artifact_ref(job_id, "plan_timeline", "timeline.json")
        conn = _open(self.paths)
        try:
            cas = CasStore(root=self.paths.cas, conn=conn)
            payload = json.loads(cas.read_bytes(ref.digest))
        finally:
            conn.close()
        return float(payload["duration_s"])

    # -- kill/resume (see module docstring) -----------------------------------

    def run_until_stage(self, stage_id: str) -> None:
        """Drive a fresh job through every stage before ``stage_id``
        synchronously, then start ``stage_id``'s own spawn in a background
        thread and return immediately, leaving a real worker subprocess
        running for ``kill_running_worker`` to kill."""
        idx = _STAGE_ORDER.index(stage_id)
        conn = _open(self.paths)
        cas = CasStore(root=self.paths.cas, conn=conn)
        artifacts = ArtifactStore(cas, conn)
        queue = JobQueue(conn)
        settings = ProjectService(conn).settings_for(self.project_id)
        pipeline = build_pipeline(_PIPELINE_ID, cas, settings)
        dispatcher = Dispatcher(
            conn, cas, artifacts, Governor(), queue, pipelines={_PIPELINE_ID: pipeline}
        )
        job_id = uuid.uuid4().hex
        queue.enqueue(job_id, self.project_id, _PIPELINE_ID)

        try:
            for expected in _STAGE_ORDER[:idx]:
                report = dispatcher.tick()
                assert report.spawned == (expected,), (
                    f"expected {expected!r} to spawn next, got "
                    f"spawned={report.spawned!r} skipped={report.skipped!r} idle={report.idle!r}"
                )
        except Exception:
            # A failure here happens before `conn` is ever handed to
            # `self._resume_conn` (below), so nothing else would close it -
            # cheap insurance in a test that deliberately drives real
            # subprocesses, mirroring test_resume.py's own connection
            # lifecycle discipline.
            conn.close()
            raise

        outcome: dict[str, TickReport] = {}

        def _drive() -> None:
            outcome["report"] = dispatcher.tick()

        driver = threading.Thread(target=_drive)
        driver.start()

        self._resume_conn = conn
        self._resume_queue = queue
        self._resume_dispatcher = dispatcher
        self._resume_driver = driver
        self._resume_job_id = job_id
        self._resume_target_stage = stage_id

    def kill_running_worker(self) -> None:
        """Kill the worker ``run_until_stage`` is currently blocked spawning
        - a genuine ``taskkill``/process-tree termination, never a faked
        death.

        ``self._resume_conn`` was opened by ``run_until_stage`` and is only
        otherwise closed by ``fast_forward_backoff``, several calls away - if
        any assertion here fails, nothing would close it (or reap the
        background driver thread) without this ``try``/``except``, leaking
        both past the test. Cheap insurance in a test that deliberately kills
        processes.
        """
        assert self._resume_dispatcher is not None, "call run_until_stage first"
        try:
            owner = f"{self._resume_job_id}:{self._resume_target_stage}"
            deadline = time.monotonic() + 20.0
            proc = None
            while time.monotonic() < deadline:
                proc = self._resume_dispatcher._running.get(owner)
                if proc is not None:
                    break
                time.sleep(0.005)
            assert proc is not None, f"worker for {owner!r} never appeared in _running within 20s"

            _kill_process_tree(proc.pid)

            assert self._resume_driver is not None
            self._resume_driver.join(timeout=60)
            assert not self._resume_driver.is_alive(), (
                "dispatcher.tick() never returned after the worker was killed"
            )
        except Exception:
            if self._resume_conn is not None:
                self._resume_conn.close()
                self._resume_conn = None
            raise

    def fast_forward_backoff(self) -> None:
        """Clear the killed stage's retry backoff so the job is immediately
        claimable again - mirrors ``test_resume.py``'s
        ``queue.requeue(job_id, available_in_s=-1)``. Closes the harness's own
        direct connection afterwards: nothing else needs it, and a fresh
        ``ytauto run`` invocation should not contend with it."""
        assert self._resume_queue is not None and self._resume_job_id is not None
        self._resume_queue.requeue(self._resume_job_id, available_in_s=-1)
        assert self._resume_conn is not None
        self._resume_conn.close()
        self._resume_conn = None


# -- the fixture ----------------------------------------------------------


@pytest.fixture()
def first_light_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Callable[..., FirstLightEnv]]:
    """Factory fixture: ``first_light_env(story=..., clips=N)``.

    Builds a real B-roll library (``N`` synthetic, distinct lavfi clips,
    ingested through ``ytauto broll add``), a real project (``ytauto project
    create``), and seeds every remaining settings key no CLI verb sets today
    (see ``_DEFAULT_SETTINGS``) directly via ``ProjectService.set_setting`` -
    the same primitive a future ``project set-setting`` command would call.
    """

    def _make(*, story: str, clips: int) -> FirstLightEnv:
        paths = AppPaths.resolve(override=tmp_path)
        binaries = _binaries()

        for i in range(clips):
            clip = _make_broll_clip(tmp_path, binaries.ffmpeg, index=i)
            rc = main(
                [
                    "--data-dir",
                    str(tmp_path),
                    "broll",
                    "add",
                    str(clip),
                    "--source-url",
                    f"lavfi://synthetic-clip-{i}",
                    "--licence",
                    "public-domain-synthetic",
                ]
            )
            assert rc == 0, f"ytauto broll add failed for synthetic clip {i}"

        story_path = tmp_path / "story.txt"
        story_path.write_text(story, encoding="utf-8")
        slug = "first-light"
        rc = main(
            [
                "--data-dir",
                str(tmp_path),
                "project",
                "create",
                "--slug",
                slug,
                "--title",
                "First Light",
                "--story",
                str(story_path),
            ]
        )
        assert rc == 0, "ytauto project create failed"

        conn = _open(paths)
        try:
            project_id = resolve_project_id(conn, slug)
            library = BrollLibrary(conn, CasStore(root=paths.cas, conn=conn))
            manifest_digest = library.write_manifest()
            projects = ProjectService(conn)
            for key, value in _DEFAULT_SETTINGS.items():
                projects.set_setting(project_id, key, value)
            projects.set_setting(project_id, "broll_manifest_digest", str(manifest_digest))

            # Fail fast, by name, if the installed `ytauto.stages` entry
            # points do not cover the real seven-stage topology - exactly
            # the stale-editable-install trap this task's own report
            # describes (a truncated install let a job "succeed" having run
            # only 2 of 7 stages, since build_pipeline only ever sees what
            # importlib.metadata advertises). Every one of criteria 1, 2, 3
            # and 4a would eventually fail on that truncation too, but each
            # as a confusing downstream assertion far from the real cause;
            # criterion 4b would not fail at all, since it only references
            # ingest_story/synthesize_speech, both of which survive a 2-of-7
            # truncation and behave correctly in isolation. This check runs
            # for every criterion, before any of them starts real work.
            cas_for_check = CasStore(root=paths.cas, conn=conn)
            pipeline = build_pipeline(_PIPELINE_ID, cas_for_check, {})
            registered = {stage.id for stage in pipeline.stages}
            assert registered == set(_STAGE_ORDER), (
                "the installed 'ytauto.stages' entry points do not match the "
                f"real pipeline topology - registered {sorted(registered)}, "
                f"expected {sorted(_STAGE_ORDER)}. Run "
                '`pip install -e ".[dev]"` to refresh a stale editable install.'
            )
        finally:
            conn.close()

        env = FirstLightEnv(paths, slug, project_id)

        original_tick = dispatcher_module.Dispatcher.tick

        def _patched_tick(self: Dispatcher) -> TickReport:
            report = original_tick(self)
            env.spawns.extend(report.spawned)
            env.skips.extend(report.skipped)
            return report

        monkeypatch.setattr(dispatcher_module.Dispatcher, "tick", _patched_tick)
        return env

    yield _make


# -- criterion 1: two playable files -----------------------------------------


@pytest.mark.integration
def test_a_pasted_story_becomes_two_playable_videos(
    first_light_env: Callable[..., FirstLightEnv],
) -> None:
    env = first_light_env(story="The train never stopped. It just kept going.", clips=4)
    assert env.run() == 0
    for name, dims in (
        ("master_1920x1080.mp4", (1920, 1080)),
        ("master_1080x1920.mp4", (1080, 1920)),
    ):
        path = env.artifact_path(name)
        assert probe_dimensions(path) == dims
        assert probe_duration(path) == pytest.approx(env.narration_seconds, abs=0.5)
        assert probe_has_audio(path)


# -- criterion 2: a re-run spawns nothing ------------------------------------


@pytest.mark.integration
def test_rerunning_the_same_job_spawns_no_workers(
    first_light_env: Callable[..., FirstLightEnv],
) -> None:
    """Every stage must be a cache hit. A single spawn means a fingerprint is unstable."""
    env = first_light_env(story="The train never stopped.", clips=4)
    env.run()
    report = env.run_again()
    assert report.spawned == (), f"unstable fingerprints in: {report.spawned}"
    assert len(report.skipped) == 7


# -- criterion 3: kill and resume --------------------------------------------


@pytest.mark.integration
def test_killing_a_worker_mid_render_resumes_at_that_stage(
    first_light_env: Callable[..., FirstLightEnv],
) -> None:
    """``spawn_count("synthesize_speech") == 1`` is over-determined by two
    independent mechanisms, either of which alone suffices to keep it at 1:
    resume tracking (a `succeeded` stage is excluded from `ready_stages` and
    never reconsidered) and, separately, the fingerprint cache (even if
    reconsidered, `tick()` would recompute the same fingerprint, find it
    already recorded, and mark it `skipped` without spawning). Two mutations
    were tried against `spawn_count` alone and neither could isolate resume
    tracking from the cache - breaking resume tracking (`_DONE_STAGE_STATUSES`
    narrowed, or `_reset_stage`'s WHERE clause widened to touch every stage
    of the job, not just the killed one) leaves the cache to silently absorb
    the corruption, so the spawn count never moves. This is Phase 1a Sec 2.3's
    documented exception: a guard that cannot be falsified by the available
    mutations pins the *observable* behaviour ("a completed stage does not
    re-run"), not the specific mechanism, and that must be recorded here
    rather than mistaken for a pin on resume tracking specifically.

    The ``resumed_job_stage_status`` assertion below is what DOES isolate
    resume tracking: it reads ``job_stages.status`` directly rather than
    counting spawns, so a stage whose status was corrupted to `pending` and
    self-healed via a cache hit shows up as `skipped`, not `succeeded` - a
    real, mutation-falsifiable difference `spawn_count` cannot see. Guard-pinned
    against the same `_reset_stage` mutation described above: with the WHERE
    clause widened, this assertion fails (`'skipped' != 'succeeded'`) even
    though `spawn_count("synthesize_speech")` stays at 1.
    """
    env = first_light_env(story="The train never stopped.", clips=4)
    env.run_until_stage("compose_landscape")
    env.kill_running_worker()
    env.fast_forward_backoff()
    assert env.run() == 0
    assert env.spawn_count("synthesize_speech") == 1, "a completed stage must not re-run"
    assert env.spawn_count("compose_landscape") == 2
    assert env.resumed_job_stage_status("synthesize_speech") == "succeeded", (
        "resume tracking must leave an already-completed stage's own status "
        "untouched - a cache hit alone would also keep spawn_count from "
        "moving, so this is what actually distinguishes the two"
    )


# -- criterion 4: selective invalidation -------------------------------------


@pytest.mark.integration
def test_changing_the_caption_colour_rerenders_only_the_compose_stages(
    first_light_env: Callable[..., FirstLightEnv],
) -> None:
    """``"accent_colour"`` is ``render_ass``'s real ``caption_style`` field
    (``core/captions/ass.py``'s ``_style_field(style, "accent_colour", ...)``
    - confirmed by reading it, not assumed). An earlier version of this test
    used ``"accent"``, a key ``render_ass`` never reads: ``_as_style`` places
    no restriction on unknown keys, so that version still proved cache
    invalidation (the settings *dict* changed, so the fingerprint changed)
    but never proved a colour change reaches the rendered output - it just
    moved a cache key. The digest comparison below is what closes that gap:
    it fails if the two ``.ass`` blobs are byte-identical, which they would
    be if the setting were silently inert.
    """
    env = first_light_env(story="The train never stopped.", clips=4)
    env.run()
    before = env.artifact_digest("compose_landscape", "captions.ass")
    env.set_setting("caption_style", {"accent_colour": "&H000000FF"})
    report = env.run_again()
    assert set(report.spawned) == {"compose_landscape", "compose_vertical"}
    after = env.artifact_digest("compose_landscape", "captions.ass")
    assert before != after, "the caption colour change never reached the rendered .ass"


@pytest.mark.integration
def test_changing_the_voice_does_not_rerun_ingest_story(
    first_light_env: Callable[..., FirstLightEnv],
) -> None:
    """The settings projection is what makes this true - see stage_support."""
    env = first_light_env(story="The train never stopped.", clips=4)
    env.run()
    env.set_setting("voice", _ALT_VOICE)
    report = env.run_again()
    assert "ingest_story" not in report.spawned
    assert "synthesize_speech" in report.spawned


# -- whole-branch review, Critical 1: the CLI alone must produce a runnable project --


@pytest.mark.integration
def test_a_cli_created_project_runs_with_no_hand_seeded_settings(tmp_path: Path) -> None:
    """The test whose absence let Critical 1 ship.

    Every other criterion in this module goes through ``first_light_env``,
    which seeds nine settings keys by hand via ``ProjectService.set_setting``
    and computes ``broll_manifest_digest`` itself - the same primitives a
    future ``ytauto project set-setting`` would call, but not primitives any
    CLI verb actually exposes today. That harness proved the pipeline works;
    it could not, and did not, prove that a *user* could get to it. A
    reviewer reproduced the gap at this branch's HEAD:

        $ ytauto project create --slug probe --title Probe --story story.txt
        created project 'probe' (3f12...)
        $ ytauto run --project probe --max-ticks 20
        ytauto run: job e02e... failed        (exit 1)
        jobs.last_error = "KeyError: 'voice'"

    ``create_project`` wrote exactly ``{story_digest, story_path}``; the
    pipeline reads nine more keys plus ``broll_manifest_digest``, every one a
    bare ``ctx.settings[...]`` subscript, so the second stage died FATAL. And
    ``broll_manifest_digest`` was not merely unset but *unobtainable*:
    ``_broll_add`` discards ``write_manifest()``'s return value, so no
    sequence of CLI commands could have produced it.

    So this test deliberately uses **no** ``set_setting`` call, no
    ``ProjectService``, and no direct ``BrollLibrary`` access - only
    ``ytauto broll add``, ``ytauto project create`` and ``ytauto run``. If
    that sequence ever stops producing two playable masters, this fails.
    ``FirstLightEnv`` is constructed directly afterwards purely as an
    artifact *reader* (it needs no fixture state for that), so this test does
    not reimplement CAS artifact resolution.
    """
    paths = AppPaths.resolve(override=tmp_path)
    binaries = _binaries()

    for i in range(2):
        clip = _make_broll_clip(tmp_path, binaries.ffmpeg, index=i)
        assert (
            main(
                [
                    "--data-dir",
                    str(tmp_path),
                    "broll",
                    "add",
                    str(clip),
                    "--source-url",
                    f"lavfi://synthetic-clip-{i}",
                    "--licence",
                    "public-domain-synthetic",
                ]
            )
            == 0
        ), f"ytauto broll add failed for synthetic clip {i}"

    story_path = tmp_path / "story.txt"
    story_path.write_text("The train never stopped.", encoding="utf-8")
    slug = "cli-only"
    assert (
        main(
            [
                "--data-dir",
                str(tmp_path),
                "project",
                "create",
                "--slug",
                slug,
                "--title",
                "CLI Only",
                "--story",
                str(story_path),
            ]
        )
        == 0
    ), "ytauto project create failed"

    conn = _open(paths)
    try:
        # The same stale-editable-install guard first_light_env makes, for the
        # same reason: build_pipeline's membership is whatever the entry-point
        # table advertises, so a truncated install "succeeds" having run some
        # prefix of the pipeline, and every assertion below would then fail
        # far from the real cause.
        pipeline = build_pipeline(_PIPELINE_ID, CasStore(root=paths.cas, conn=conn), {})
        registered = {stage.id for stage in pipeline.stages}
        assert registered == set(_STAGE_ORDER), (
            "the installed 'ytauto.stages' entry points do not match the real "
            f"pipeline topology - registered {sorted(registered)}. Run "
            '`pip install -e ".[dev]"` to refresh a stale editable install.'
        )
        project_id = resolve_project_id(conn, slug)
    finally:
        conn.close()

    # --output-dir keeps this hermetic - see FirstLightEnv._cli_run's comment
    # for why the auto-detected Videos/Downloads location is not used here.
    rc = main(
        [
            "--data-dir",
            str(tmp_path),
            "run",
            "--project",
            slug,
            "--output-dir",
            str(tmp_path / "output"),
        ]
    )
    assert rc == 0, (
        f"ytauto run exited {rc} for a project created by ytauto project create "
        f"with no further configuration; see {paths.logs / 'ytauto.jsonl'}"
    )

    env = FirstLightEnv(paths, slug, project_id)
    for name, dims in (
        ("master_1920x1080.mp4", (1920, 1080)),
        ("master_1080x1920.mp4", (1080, 1920)),
    ):
        path = env.artifact_path(name)
        assert probe_dimensions(path) == dims
        assert probe_has_audio(path)
