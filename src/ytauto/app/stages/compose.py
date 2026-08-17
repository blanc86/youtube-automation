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
every line of this module rather than duplicating it. Only
``make_compose_landscape`` is wired to an entry point here; Task 12 adds its
own factory binding the vertical canvas's arguments.

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
from collections.abc import Mapping
from pathlib import Path

from ytauto.app.stage_support import stage_fingerprint
from ytauto.core.captions.ass import render_ass
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.core.pipeline.timeline import CaptionGroup, Timeline
from ytauto.infra.cas.store import CasStore
from ytauto.infra.ffmpeg.compose import ComposeClip, compose_args
from ytauto.infra.ffmpeg.locator import FfmpegBinaries, locate
from ytauto.infra.ffmpeg.probe import probe

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
``_DEFAULT_FONT_SIZE``, not by leaning on that default (this value is always
passed explicitly, every call, regardless of what a caller's
``caption_style`` supplies) but because 96 was already sized correctly for
the vertical canvas alone - it was only ever wrong for landscape."""

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

    version = 1
    depends_on: tuple[str, ...] = ("plan_timeline", "select_broll", "synthesize_speech")
    settings_keys: tuple[str, ...] = ("broll_manifest_digest", "caption_style", "encoder")
    gpu_pool = "gpu_encode"
    """Read by ``Dispatcher._spawn`` in place of the default ``gpu_compute``
    pool (``getattr(stage, "gpu_pool", _GPU_POOL)``) - this stage encodes
    video, it never runs a compute model like Whisper, so it competes for a
    separate lease pool rather than one a future ASR-based ``Transcriber``
    would also need. A refused ``gpu_encode`` lease is a normal requeue, not
    a failure - ``Dispatcher._spawn`` already handles that generically for
    any pool."""

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

    def _resolve_encoder(self, encoder_setting: str, ffmpeg_binaries: FfmpegBinaries) -> str:
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

        Raises:
            ProviderError: FATAL, if an explicit override names an encoder
                this ffmpeg build does not expose.
        """
        capabilities = probe(ffmpeg_binaries)
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

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        """Read the three upstream artifacts and the manifest, render
        captions, run ffmpeg once, and stage the master video plus the
        ``.ass`` it burned in.

        Raises:
            ProviderError: FATAL, from ``_resolve_clips`` (a manifest/segments
                mismatch), ``_resolve_encoder`` (an unavailable explicit
                encoder override), or a non-zero ffmpeg exit - see
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
        # Always overridden, never merely defaulted-if-absent: this task's
        # first bound decision is that font_size must never rely on
        # render_ass's own canvas-agnostic default, for either canvas, on
        # every render - not only when a caller's caption_style happens to
        # omit it. See _FONT_SIZE_DIVISOR's own docstring.
        style["font_size"] = self._height // _FONT_SIZE_DIVISOR
        ass_text = render_ass(timeline, width=self._width, height=self._height, style=style)

        ctx.workdir.mkdir(parents=True, exist_ok=True)
        ass_bytes = ass_text.encode("utf-8")
        (ctx.workdir / _ASS_FILENAME).write_bytes(ass_bytes)
        ass_digest = self._cas.stage_file(ass_bytes, kind="text")

        encoder_setting = _as_encoder_setting(ctx.settings["encoder"])
        binaries = locate()
        encoder = self._resolve_encoder(encoder_setting, binaries)

        out_path = ctx.workdir / self._artifact_name
        args = compose_args(
            clips=clips,
            ass_path=Path(_ASS_FILENAME),
            audio_path=audio_path,
            out_path=out_path,
            width=self._width,
            height=self._height,
            encoder=encoder,
        )
        result = subprocess.run(
            [str(binaries.ffmpeg), *args],
            cwd=ctx.workdir,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=_ENCODE_TIMEOUT_S,
            check=False,
        )
        if result.returncode != 0:
            log_path = ctx.workdir / _FFMPEG_STDERR_LOG
            log_path.write_text(result.stderr, encoding="utf-8")
            raise ProviderError(
                f"ffmpeg exited {result.returncode} composing {self._artifact_name}; "
                f"see {log_path} for the full diagnostic",
                provider_id=PROVIDER_ID,
                kind=ErrorKind.FATAL,
            )

        video_digest = self._cas.stage_file(out_path.read_bytes(), kind="video")

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
