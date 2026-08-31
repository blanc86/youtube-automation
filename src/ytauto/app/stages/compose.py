"""``compose_landscape``/``compose_vertical``: the pipeline's final stage -
the first one that shells out to ffmpeg to render, and the one every earlier
task exists to feed.

**No provider, no port.** Unlike ``select_broll``/``synthesize_speech``/
``transcribe``, there is no ``VisualStrategy``-style Protocol here for
``ytauto.app`` to depend on and a ``providers/`` package to implement it.
ffmpeg is infrastructure - a fixed external binary this project always shells
out to the same way - not a swappable strategy a future task might replace
with a second implementation the way Piper could replace edge-tts. So
``ComposeStage`` is constructed directly, in ``app/``, with no injected
Protocol to satisfy: the pure argument-building half lives in
``infra.ffmpeg.compose.compose_args`` (see that module's docstring for the
one-ffmpeg-invocation design and the Windows drive-letter guard), and this
module is the thin orchestration around it - reading three upstream
artifacts and a manifest, resolving B-roll clip ids to this canvas's own
digest, rendering captions at this canvas's own resolution, running ffmpeg,
and staging the result.

**One class, two entry points.** ``ComposeStage`` takes its canvas
dimensions, stage id, output artifact name, and which manifest digest field
names its canvas as constructor arguments - identical behaviour, different
literals - so ``compose_landscape`` and ``compose_vertical`` (Task 12) share
every line of this module rather than duplicating it. Both
``make_compose_landscape`` and ``make_compose_vertical`` are wired to entry
points (``pyproject.toml``'s ``[project.entry-points."ytauto.stages"]``);
neither needed any change to ``ComposeStage`` itself.

**``ProviderError``, not ``RenderError``, for a failed ffmpeg exit.**
``core.errors.RenderError`` exists and is already used by
``infra.broll._run_normalise`` - but that call happens inside the CLI
process (``ytauto broll add``), entirely outside the ``Stage``/``run_stage``
machinery. A ``Stage.run`` failure has to travel back to the dispatcher
through ``app.scheduler.runner.run_stage``, which special-cases
``ProviderError`` specifically to preserve ``kind`` (FATAL here - a bad
filter graph or a missing codec does not fix itself on retry),
``provider_id``, and ``retry_after_s``; a bare ``RenderError`` would fall
through ``run_stage``'s generic ``except Exception`` branch, land on
``ErrorKind.FATAL`` anyway by default, but lose the explicit
provider-attributed shape every other stage in this codebase reports
failures in. ``ProviderError(kind=ErrorKind.FATAL)`` is the one used below.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path

from ytauto.app.stage_support import stage_fingerprint
from ytauto.core.captions.ass import render_ass
from ytauto.core.errors import ConfigurationError, ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.core.pipeline.timeline import CaptionGroup, Timeline
from ytauto.infra.cas.store import CasStore
from ytauto.infra.ffmpeg.compose import ComposeClip, MusicBed, compose_args
from ytauto.infra.ffmpeg.locator import locate
from ytauto.infra.ffmpeg.probe import FfmpegCapabilities, probe

PROVIDER_ID = "ffmpeg"
"""Literal, fed to ``stage_fingerprint`` and to every ``ProviderError`` this
module raises - see ``select_broll.py``'s module docstring for why stage
identity is always a literal constant rather than anything read off an
injected object. There is no injected object here at all (see this module's
own docstring), so the literal is simply this stage's only honest choice."""

PROVIDER_VERSION = "1"

_ASS_FILENAME = "captions.ass"
"""Bare filename ``compose_args`` receives and ffmpeg is run against with
``cwd`` set to ``ctx.workdir`` - see ``infra.ffmpeg.compose``'s module
docstring for why this sidesteps the Windows drive-letter colon entirely
rather than escaping it."""

_FONT_SIZE_DIVISOR = 20
"""``height // 20`` is this task's resolved choice for Task 8's deferred
``font_size`` decision: roughly 5% of frame height on either canvas, so
captions read the same visual size on both. Landscape (1080 tall) gets 54;
vertical (1920 tall) gets 96 - which happens to equal ``render_ass``'s own
``_DEFAULT_FONT_SIZE``, but not by leaning on that default: this value is
always the one ``style.setdefault("font_size", ...)`` falls back to in
``run()`` when ``caption_style`` does not name one of its own - never
render_ass's internal 96, which is wrong for the 1080-tall landscape canvas.
An operator-supplied ``caption_style["font_size"]`` still wins over this
default (Task 11's review, Important #5): overwriting it unconditionally
made ``font_size`` a declared ``settings_keys`` member the fingerprint
depended on while the code guaranteed it never actually changed the
rendered output - a real cache-key/behaviour mismatch, not merely
inelegant."""

_FALLBACK_ENCODER = "libx264"
"""Retried once, automatically, when the primary encoder was chosen via
``encoder == "auto"`` and it was a hardware one that then failed at ffmpeg
run time - a driver mismatch, VRAM exhaustion, or (on a consumer card like
this project's own RTX 3050) NVENC's concurrent-session limit already held
by another process. ``FfmpegCapabilities.best_h264_encoder`` only proves an
encoder is *listed*; it says nothing about whether the hardware will accept
a real encode this run, which is a materially different question this
constant exists to answer defensively. Never applied when the operator named
an encoder explicitly (``encoder != "auto"``) - retrying past an explicit
choice would silently override it, and libx264's own output is not what
they asked for."""

_FFMPEG_STDERR_LOG = "ffmpeg-stderr.log"
"""Where this stage writes ffmpeg's captured stderr on a failed compose, and
what a FATAL ``ProviderError`` names in its message.

Not literally Task 1's ``stderr.attempt-{attempts}.log`` naming, because
``Stage.run(ctx, emit)`` is never handed an attempt count - ``JobContext``
carries ``job_id``/``project_id``/``settings``/``inputs``/``workdir`` and
nothing else, and the attempt number lives only in the dispatcher's
``ClaimedJob`` and the worker-protocol ``correlation_id``, neither of which
reaches a stage's own code. Fabricating that filename from inside this stage
would mean guessing a number this stage has no reliable way to know, which
risks naming a path that is subtly wrong rather than one that is always
correct. This name is not: it is fully controlled by this stage, written
immediately before the ``ProviderError`` that names it, so the path in the
message is guaranteed to exist and be current - the same debugging goal
Task 1's convention serves, achieved with a name this code can actually
promise."""

_ENCODE_TIMEOUT_S = 900.0
"""Generous relative to Phase 2a's short faceless-video runtime: a full
concat-plus-caption-burn encode is more work than one B-roll clip's ingest
normalisation (``infra.broll``'s own 600s), never something on a
request-response latency budget."""


def _as_digest(value: object, *, key: str) -> ContentHash:
    """Narrow one ``ctx.settings`` value to a ``ContentHash``. Mirrors
    ``select_broll.py``'s identical helper.

    Raises:
        TypeError: ``value`` is not a ``str``.
    """
    if not isinstance(value, str):
        raise TypeError(f"expected a str digest for {key!r}, got {type(value).__name__}")
    return ContentHash(value)


def _as_style(value: object) -> Mapping[str, object]:
    """Narrow one ``ctx.settings`` value to the ``style`` mapping
    ``render_ass`` expects.

    Raises:
        TypeError: ``value`` is not a ``Mapping``.
    """
    if not isinstance(value, Mapping):
        raise TypeError(f"expected a mapping for caption_style, got {type(value).__name__}")
    return value


def _as_encoder_setting(value: object) -> str:
    """Narrow one ``ctx.settings`` value to the ``encoder`` string.

    Raises:
        TypeError: ``value`` is not a ``str``.
    """
    if not isinstance(value, str):
        raise TypeError(f"expected a str for encoder, got {type(value).__name__}")
    return value


def _as_float(value: object) -> float:
    """Narrow one decoded-JSON value to ``float``. An ``int`` is accepted and
    widened, mirroring ``core.pipeline.timeline``'s own ``_require_float`` -
    ``json.loads`` hands back an ``int`` for any whole-number literal, and a
    hand-edited or synthetic ``timeline.json``/``segments.json`` is exactly
    where that shows up.

    Raises:
        TypeError: ``value`` is neither ``float`` nor ``int``.
    """
    if isinstance(value, bool):
        raise TypeError(f"expected a float, got bool: {value!r}")
    if isinstance(value, int | float):
        return float(value)
    raise TypeError(f"expected a float, got {type(value).__name__}: {value!r}")


def _decode_timeline(raw: Mapping[str, object]) -> Timeline:
    """Reconstruct a ``Timeline`` from ``timeline.json``'s
    ``json.dumps(asdict(timeline))`` shape (Task 7).

    Only ``groups`` is read into real ``CaptionGroup`` objects - ``segments``
    is decoded as an empty tuple, since ``render_ass`` never reads
    ``Timeline.segments`` at all (B-roll segmentation is a different concern
    it has no part in; per its own module docstring) and this stage's own
    B-roll placement comes from ``segments.json``, not from re-deriving it
    out of ``timeline.json``.
    """
    groups_raw = raw["groups"]
    assert isinstance(groups_raw, list)  # narrows for mypy; json.loads always gives a list here
    groups = tuple(
        CaptionGroup(
            start_s=float(group["start_s"]),
            end_s=float(group["end_s"]),
            words=tuple((str(word[0]), float(word[1]), float(word[2])) for word in group["words"]),
        )
        for group in groups_raw
    )
    return Timeline(duration_s=_as_float(raw["duration_s"]), groups=groups, segments=())


class ComposeStage:
    """Renders one canvas's master video: trim, concat, burn captions, mux
    narration, in a single ffmpeg pass.

    Reads three upstream artifacts - ``timeline.json`` (Task 7, for
    captions), ``segments.json`` (Task 10, for B-roll placement) and
    ``narration.mp3`` (Task 5, for audio) - plus the B-roll manifest, whose
    digest arrives as ``ctx.settings["broll_manifest_digest"]`` exactly like
    ``select_broll`` reads it, per this task's second bound decision (see the
    module docstring). ``digest_field`` is what makes clip resolution
    canvas-specific: ``"normalised_landscape_digest"`` here,
    ``"normalised_vertical_digest"`` for Task 12's vertical stage, against
    the same manifest entries ``select_broll`` already read ``clip_id`` and
    ``duration_s`` from.
    """

    version = 5
    """Bump whenever this stage's *rendered output* changes - including when the
    change is in a dependency rather than in this file.

    ``render_ass`` (``core.captions.ass``) is the trap: it decides how every
    caption looks and when it appears, yet it reaches no fingerprint. Only
    ``stage_id``, this ``version``, the provider constants, the input digests
    and the declared ``settings_keys`` do. So a caption change with no bump
    here is served straight from cache and the operator sees the old video
    while believing the fix shipped - measured, not hypothetical: the
    event-tiling fix re-ran in 1.8s and returned byte-identical masters.

    v2: captions tile with no blank frames (was 6.99s blank of 21.5s) and sit
    middle-centre rather than bottom.

    v3: an optional music bed is mixed under the narration. The audio half of
    the filter graph is new, so even a project with no track selected renders
    through changed code and must not be served a v2 master.

    v4: the bed is normalised to a standard level before the operator's trim.
    Without it, `music_gain_db` attenuated a track whose own level was unknown
    - three legitimate tracks spanning 24 dB of input level meant the same
    setting produced an inaudible bed for one and a reasonable one for
    another, which is what "the background music is not working" turned out to
    be.

    v5: no format pin on the mix branches. Pinning both to stereo so amix's
    inputs agreed also pushed a mono narration through ffmpeg's
    mono-to-stereo rematrix, and the finished master came back 2.5 dB quieter
    than the same render with no music at all - the voice attenuated by a
    change that was supposed to be about the music."""

    depends_on: tuple[str, ...] = ("plan_timeline", "select_broll", "synthesize_speech")
    settings_keys: tuple[str, ...] = (
        "broll_manifest_digest",
        "caption_style",
        "encoder",
        # Both music keys belong here for the reason `version` above exists:
        # they change the rendered file. Omitting either would serve a cached
        # master from before the bed was added - the exact failure the caption
        # fix hit, where a corrected render returned byte-identical output in
        # 1.8s because the thing that changed reached no fingerprint.
        #
        # `music_digest`, not `music_track_id`: this stage runs in a worker
        # process with a CasStore and no database connection, so a track *id*
        # is not something it can resolve. `refresh_run_settings` turns the id
        # into a digest per run, exactly as it does for the B-roll manifest,
        # and the digest is also the more honest fingerprint input - it names
        # the audio itself rather than a row that could later point elsewhere.
        "music_digest",
        "music_gain_db",
    )
    gpu_pool = "gpu_encode"
    """Read directly by ``Dispatcher._spawn`` (a required ``Stage`` Protocol
    member, per Task 11's review - see ``core.pipeline.stage.Stage.gpu_pool``
    for the full account) in place of the default ``gpu_compute`` - this
    stage encodes video, it never runs a compute model like Whisper, so it
    competes for a separate lease pool rather than one a future ASR-based
    ``Transcriber`` would also need. A refused ``gpu_encode`` lease is a
    normal requeue, not a failure - ``Dispatcher._spawn`` already handles
    that generically for any pool."""

    def __init__(
        self,
        *,
        cas: CasStore,
        stage_id: str,
        width: int,
        height: int,
        artifact_name: str,
        digest_field: str,
    ) -> None:
        self._cas = cas
        self.id = stage_id
        self._width = width
        self._height = height
        self._artifact_name = artifact_name
        self._digest_field = digest_field

    def fingerprint(self, ctx: JobContext) -> str:
        return stage_fingerprint(
            self, ctx, provider_id=PROVIDER_ID, provider_version=PROVIDER_VERSION
        )

    def _resolve_clips(
        self, segments: list[Mapping[str, object]], manifest_raw: list[Mapping[str, object]]
    ) -> list[ComposeClip]:
        """Resolve each ``segments.json`` entry's ``clip_id`` to this
        stage's own canvas digest and a real CAS path.

        ``segments.json`` (Task 10) deliberately names ``clip_id``, never a
        digest, precisely so one ``select_broll`` result serves both compose
        stages - this is the one place that indirection is resolved, against
        ``self._digest_field`` rather than the other canvas's.

        Raises:
            ProviderError: FATAL, if a segment's ``clip_id`` has no entry in
                the manifest for this canvas - a manifest that has drifted
                out of sync with ``segments.json`` (e.g. a clip removed from
                the library after selection ran). No retry regenerates a
                clip that is not there.
        """
        digests_by_clip = {
            str(entry["clip_id"]): str(entry[self._digest_field]) for entry in manifest_raw
        }
        clips: list[ComposeClip] = []
        for segment in segments:
            clip_id = str(segment["clip_id"])
            digest_str = digests_by_clip.get(clip_id)
            if digest_str is None:
                raise ProviderError(
                    f"segment references clip_id {clip_id!r}, which has no "
                    f"{self._digest_field!r} entry in the B-roll manifest for this "
                    f"canvas ({self._width}x{self._height}); the manifest may have "
                    "changed since select_broll ran",
                    provider_id=PROVIDER_ID,
                    kind=ErrorKind.FATAL,
                )
            clip_path = self._cas.path_for(ContentHash(digest_str))
            clips.append(
                ComposeClip(
                    path=clip_path,
                    in_point_s=_as_float(segment["in_point_s"]),
                    duration_s=_as_float(segment["duration_s"]),
                )
            )
        return clips

    def _resolve_music(self, ctx: JobContext, clips: list[ComposeClip]) -> MusicBed | None:
        """Turn the two music settings into a ``MusicBed``, or ``None``.

        An empty ``music_digest`` means no bed, which is the default and the
        ordinary case: the graph is then byte-for-byte the one that existed
        before music, and the narration stream is mapped directly.

        The bed's fade is timed against the sum of the clip durations rather
        than the narration's own length, because that sum *is* the video: the
        timeline ends at the last word and ``-shortest`` cuts the mux there,
        so this is the duration a viewer experiences.

        Raises:
            ProviderError: FATAL, if ``music_digest`` names a blob the CAS
                does not hold. A track evicted or removed between enqueue and
                render is not something a retry recovers.
        """
        digest = str(ctx.settings.get("music_digest", "") or "")
        if not digest:
            return None

        music_path = self._cas.path_for(ContentHash(digest))
        if not music_path.is_file():
            raise ProviderError(
                f"the selected music track ({digest[:12]}) is not in the content store; "
                "it may have been evicted or removed since this run was enqueued",
                provider_id=PROVIDER_ID,
                kind=ErrorKind.FATAL,
            )

        return MusicBed(
            path=music_path,
            trim_db=_as_float(ctx.settings.get("music_gain_db", 0.0)),
            total_duration_s=sum(clip.duration_s for clip in clips),
        )

    def _resolve_encoder(self, encoder_setting: str, capabilities: FfmpegCapabilities) -> str:
        """Turn the ``encoder`` project setting into a real, available encoder name.

        ``"auto"`` (the ordinary case) defers to
        ``infra.ffmpeg.probe.FfmpegCapabilities.best_h264_encoder`` - the
        fallback chain this task's brief calls for (NVENC, then QSV, then
        libx264), so a machine with no NVENC (CI's macOS runners) still
        renders rather than failing. Any other value is treated as an
        explicit operator override - forcing ``libx264`` for portability over
        hardware acceleration, say - and is used as-is if this ffmpeg build
        actually exposes it. Either way, ``encoder`` is one of this stage's
        ``settings_keys``: whichever branch runs, a changed value is a
        changed rendered file and must invalidate the cache.

        This only proves the chosen encoder is *listed* by ``ffmpeg
        -encoders`` - it says nothing about whether the hardware will accept
        a real encode this run (a driver mismatch, VRAM exhaustion, or a
        consumer card's NVENC session limit already held by another process
        all pass this check and then fail at ``run()``'s actual ffmpeg
        invocation). ``run()`` is what retries with ``_FALLBACK_ENCODER`` on
        that runtime failure; this method only ever answers "is it listed".

        Raises:
            ProviderError: FATAL, if an explicit override names an encoder
                this ffmpeg build does not expose.
        """
        if encoder_setting == "auto":
            return capabilities.best_h264_encoder()
        if encoder_setting in capabilities.encoders:
            return encoder_setting
        raise ProviderError(
            f"encoder {encoder_setting!r} was requested but this ffmpeg build does "
            f"not expose it (has: {sorted(capabilities.encoders)}); use 'auto' to let "
            "the fallback chain pick one",
            provider_id=PROVIDER_ID,
            kind=ErrorKind.FATAL,
        )

    def _run_ffmpeg(
        self, ffmpeg: Path, args: list[str], cwd: Path
    ) -> subprocess.CompletedProcess[str]:
        """Run one ffmpeg invocation, stderr and stdout both captured.

        ``subprocess.run`` with captured output, never a manual ``Popen`` -
        this project has already shipped one leaked-pipe bug, and
        ``ResourceWarning``/``PytestUnraisableExceptionWarning`` are promoted
        to errors specifically to catch it happening again (see
        ``infra.broll._run_normalise``, which states the same rule).

        Raises:
            subprocess.TimeoutExpired: the encode did not finish within
                ``_ENCODE_TIMEOUT_S``.
            OSError: ``ffmpeg`` cannot be executed.
        """
        return subprocess.run(
            [str(ffmpeg), *args],
            cwd=cwd,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=_ENCODE_TIMEOUT_S,
            check=False,
        )

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        """Read the three upstream artifacts and the manifest, render
        captions, run ffmpeg once (retrying at most once, on a hardware
        encoder's own runtime failure - see ``_FALLBACK_ENCODER``), and stage
        the master video plus the ``.ass`` it burned in.

        **The master is moved into the CAS, not copied, and nothing else in
        the workdir is touched.** ffmpeg has to write its output to a real
        path, and that path is ``ctx.workdir``; before this, the render was
        then read into memory in full and staged with ``stage_file``, leaving
        the whole master sitting in ``<data>/assets/work/<job>/<stage>/``
        forever. Nothing in ``src/`` ever removed that tree, and ``Evictor``
        walks ``cas_root`` and ``cas_objects`` only - so the largest files
        this application produces, two per render, sat entirely outside the
        40 GiB ceiling. At a couple of renders an hour that grows without
        bound. ``stage_path(..., move=True)`` makes storing and cleanup one
        step, and as a bonus never materialises the file in RAM.

        Deleting only the staged media file was chosen over deleting the
        workdir at stage completion, and over deleting it when the *job*
        reaches a terminal state. The workdir is also where the diagnostics
        live: ``ffmpeg-stderr.log`` written just above, and Task 1's
        per-attempt ``stderr.attempt-N.log`` written by the dispatcher around
        this stage. Clearing the directory on stage completion would race
        those; clearing it on job completion would take the FAILED case too,
        which is precisely the case whose logs someone needs. This delete
        only ever runs after ffmpeg *succeeded* and the bytes are safely
        content-addressed, so a failed render keeps everything - including
        the partial output ffmpeg left behind. What remains after a success
        is kilobytes of text, which is a bound, not a leak.

        Raises:
            ConfigurationError: this ffmpeg build has no ``ass`` filter
                (libass) at all - checked up front, before any of the CAS
                reads below, so a build that can never burn captions fails
                immediately rather than after doing real work.
            ProviderError: FATAL, from ``_resolve_clips`` (a manifest/segments
                mismatch), ``_resolve_encoder`` (an unavailable explicit
                encoder override), or a non-zero ffmpeg exit (after the one
                automatic fallback retry, where applicable) - see
                ``_FFMPEG_STDERR_LOG``'s own docstring for why the log path
                named in that last message is not literally Task 1's
                ``stderr.attempt-N.log``.
            KeyError: ``ctx.settings`` is missing a declared settings key, or
                a decoded artifact is missing an expected field - a malformed
                or hand-edited upstream artifact. ``run_stage`` translates
                this into a FATAL worker-protocol error, so it is not caught
                specially.
            TypeError: a settings value is present but the wrong type.
        """
        binaries = locate()
        capabilities = probe(binaries)
        if not capabilities.has_subtitle_burn_in():
            raise ConfigurationError(
                f"{binaries.ffmpeg} (version {binaries.version}) has no 'ass' filter "
                "(libass); this build cannot burn in captions at all - install a build "
                "with libass support"
            )

        segments_ref = ctx.input("select_broll", "segments.json")
        segments: list[Mapping[str, object]] = json.loads(self._cas.read_bytes(segments_ref.digest))

        timeline_ref = ctx.input("plan_timeline", "timeline.json")
        timeline_raw: Mapping[str, object] = json.loads(self._cas.read_bytes(timeline_ref.digest))
        timeline = _decode_timeline(timeline_raw)

        narration_ref = ctx.input("synthesize_speech", "narration.mp3")
        audio_path = self._cas.path_for(narration_ref.digest)

        manifest_digest = _as_digest(
            ctx.settings["broll_manifest_digest"], key="broll_manifest_digest"
        )
        manifest_raw: list[Mapping[str, object]] = json.loads(self._cas.read_bytes(manifest_digest))
        clips = self._resolve_clips(segments, manifest_raw)

        style = dict(_as_style(ctx.settings["caption_style"]))
        # setdefault, not an unconditional overwrite: font_size must never
        # rely on render_ass's own canvas-agnostic default (this task's first
        # bound decision), but an operator-supplied caption_style["font_size"]
        # must still win - see _FONT_SIZE_DIVISOR's own docstring and Task
        # 11's review, Important #5.
        style.setdefault("font_size", self._height // _FONT_SIZE_DIVISOR)
        ass_text = render_ass(timeline, width=self._width, height=self._height, style=style)

        ctx.workdir.mkdir(parents=True, exist_ok=True)
        ass_bytes = ass_text.encode("utf-8")
        (ctx.workdir / _ASS_FILENAME).write_bytes(ass_bytes)
        # Not staged into the CAS yet - see below, after ffmpeg's own
        # success is confirmed. This workdir copy is the one ffmpeg itself
        # needs to read (cwd=ctx.workdir, bare filename - the Windows
        # drive-letter guard); it has nothing to do with the CAS blob.

        encoder_setting = _as_encoder_setting(ctx.settings["encoder"])
        encoder = self._resolve_encoder(encoder_setting, capabilities)

        music = self._resolve_music(ctx, clips)

        out_path = ctx.workdir / self._artifact_name

        def _args_for(chosen_encoder: str) -> list[str]:
            return compose_args(
                clips=clips,
                ass_path=Path(_ASS_FILENAME),
                audio_path=audio_path,
                out_path=out_path,
                width=self._width,
                height=self._height,
                encoder=chosen_encoder,
                music=music,
            )

        used_encoder = encoder
        result = self._run_ffmpeg(binaries.ffmpeg, _args_for(encoder), ctx.workdir)
        diagnostic = result.stderr

        # One automatic retry, hardware-encoder-to-libx264, only when the
        # encoder was auto-selected: FfmpegCapabilities.best_h264_encoder
        # only proves an encoder is *listed*, never that the hardware will
        # accept a real encode this run (a driver mismatch, VRAM exhaustion,
        # or - on this project's own RTX 3050 - NVENC's concurrent-session
        # limit already held by another process all pass that check and fail
        # here instead). Never applied to an operator's explicit choice -
        # that would silently override it.
        if result.returncode != 0 and encoder_setting == "auto" and encoder != _FALLBACK_ENCODER:
            print(
                f"ffmpeg exited {result.returncode} with encoder {encoder!r}; "
                f"retrying once with {_FALLBACK_ENCODER!r}",
                file=sys.stderr,
            )
            fallback_result = self._run_ffmpeg(
                binaries.ffmpeg, _args_for(_FALLBACK_ENCODER), ctx.workdir
            )
            diagnostic = (
                f"primary encoder {encoder!r} failed (exit {result.returncode}):\n"
                f"{result.stderr}\n\n"
                f"fallback encoder {_FALLBACK_ENCODER!r} "
                f"{'succeeded' if fallback_result.returncode == 0 else 'also failed'} "
                f"(exit {fallback_result.returncode}):\n{fallback_result.stderr}"
            )
            result = fallback_result
            used_encoder = _FALLBACK_ENCODER

        if result.returncode != 0:
            log_path = ctx.workdir / _FFMPEG_STDERR_LOG
            log_path.write_text(diagnostic, encoding="utf-8")
            # Echoed to this worker's own stderr too, not only the
            # self-owned log file: Task 1 redirects the whole worker
            # process's stderr to ctx.workdir/stderr.attempt-N.log, and
            # capture_output=True above otherwise diverts ffmpeg's real
            # diagnostic away from that stream entirely - the dispatcher's
            # own docstring anticipated exactly this ("ffmpeg - Phase 2a's
            # first real stderr writer"). The two logs are complementary,
            # not redundant: this one is guaranteed correctly named (see
            # _FFMPEG_STDERR_LOG's docstring), that one is where an operator
            # already watching a live worker's stderr will see it first.
            print(diagnostic, file=sys.stderr)
            raise ProviderError(
                f"ffmpeg exited {result.returncode} composing {self._artifact_name} "
                f"(encoder {used_encoder!r}); see {log_path} for the full diagnostic",
                provider_id=PROVIDER_ID,
                kind=ErrorKind.FATAL,
            )

        # Staged into the CAS only now that ffmpeg has actually succeeded -
        # staging it earlier left an orphaned blob file (no cas_objects row,
        # so the evictor - which walks DB rows, not the filesystem - could
        # never reclaim it) on every FATAL failure between the stage and
        # here (Task 11's review, Minor).
        ass_digest = self._cas.stage_file(ass_bytes, kind="text")
        # move=True: the master goes INTO the CAS rather than being copied
        # into it, so the workdir does not keep a full-size duplicate. See
        # this method's docstring for why only this file is removed and the
        # rest of the workdir is deliberately left alone.
        video_digest = self._cas.stage_path(out_path, kind="video", move=True)

        return StageResult(
            artifacts=(
                ArtifactRef(name=self._artifact_name, kind="video", digest=video_digest),
                ArtifactRef(name=_ASS_FILENAME, kind="text", digest=ass_digest),
            )
        )


_LANDSCAPE_WIDTH = 1920
_LANDSCAPE_HEIGHT = 1080
_LANDSCAPE_ARTIFACT = "master_1920x1080.mp4"


def make_compose_landscape(*, cas: CasStore, settings: Mapping[str, object]) -> ComposeStage:
    """Entry point ``story_video:compose_landscape``.

    ``settings`` is accepted, not used at construction time - every stage
    factory shares the ``(*, cas, settings) -> Stage`` shape
    (``app.registry.build_stage``'s contract) - but unused here: everything
    this stage's behaviour depends on is read from ``ctx.settings`` at run
    time instead (``ComposeStage.run``), never baked into the object at
    construction, for the same reason every other stage's factory in this
    codebase does the same - see ``app.registry.build_stage``'s own
    docstring on why a fingerprint must never depend on anything a factory
    decided from settings.
    """
    return ComposeStage(
        cas=cas,
        stage_id="compose_landscape",
        width=_LANDSCAPE_WIDTH,
        height=_LANDSCAPE_HEIGHT,
        artifact_name=_LANDSCAPE_ARTIFACT,
        digest_field="normalised_landscape_digest",
    )


_VERTICAL_WIDTH = 1080
_VERTICAL_HEIGHT = 1920
_VERTICAL_ARTIFACT = "master_1080x1920.mp4"


def make_compose_vertical(*, cas: CasStore, settings: Mapping[str, object]) -> ComposeStage:
    """Entry point ``story_video:compose_vertical`` (Task 12).

    Identical to ``make_compose_landscape`` in every respect but the
    literals: Shorts' 1080x1920 portrait canvas, its own output artifact
    name, and ``normalised_vertical_digest`` as the manifest field
    ``ComposeStage._resolve_clips`` reads clip digests from - see that
    method's own docstring for why this indirection exists, and
    ``ComposeStage``'s module docstring for why one class serves both
    canvases rather than two near-duplicates. ``settings`` is accepted and
    unused for the same reason ``make_compose_landscape`` accepts and
    ignores it.
    """
    return ComposeStage(
        cas=cas,
        stage_id="compose_vertical",
        width=_VERTICAL_WIDTH,
        height=_VERTICAL_HEIGHT,
        artifact_name=_VERTICAL_ARTIFACT,
        digest_field="normalised_vertical_digest",
    )
