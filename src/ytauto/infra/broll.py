"""B-roll ingest: probe, dual-canvas normalisation, provenance, manifest.

This is where the project's DMCA protection lives: every clip records where it
came from and under what licence at the moment it enters the library, via the
``source_url``/``licence``/``attribution``/``notes`` columns migration 004
added to ``broll_clips``. ``BrollLibrary.add`` refuses a blank ``source_url``
or ``licence`` rather than defaulting one in, and the CLI mirrors that by
making both required flags - an optional licence would be blank on every clip
within a week.

Two normalised renditions are produced per clip, **both transcoded from the
original source**, never one cropped from the other: a 9:16 crop of a 1920x1080
source is 607x1080 and needs a 1.78x upscale, while normalising each canvas
straight from the source keeps both optimal. That is why ``broll_clips`` carries
two digest columns and why ``add`` calls ``normalise_clip`` twice.

Ingest is not a pipeline stage - there is no entry point, no ``Stage`` class, no
``make_stage``. It runs synchronously inside the CLI process (``ytauto broll
add``), which is also why it is the one place outside a worker that is allowed
to touch SQLite directly: ``CasStore.put_file``/``put_bytes`` are documented as
parent-side writes, and this *is* the parent.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
import tempfile
import uuid
from pathlib import Path

from ytauto.core.errors import RenderError, ValidationError
from ytauto.core.models.content_hash import hash_file
from ytauto.infra.cas.store import CasStore, ContentHash
from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import transaction
from ytauto.infra.ffmpeg.locator import locate
from ytauto.infra.ffmpeg.media_probe import probe_media

LANDSCAPE: tuple[int, int] = (1920, 1080)
VERTICAL: tuple[int, int] = (1080, 1920)

_TARGET_FPS = "30"
_TARGET_PIX_FMT = "yuv420p"
_VIDEO_KIND = "video"
_MANIFEST_KIND = "broll_manifest"

# Generous relative to a short B-roll source: ingest is an offline, one-shot
# CLI operation, not something on a request-response latency budget, and a
# too-short timeout would fail large legitimately-slow sources rather than
# genuinely hung encodes.
_ENCODE_TIMEOUT_S = 600.0


def normalise_clip(src: Path, *, width: int, height: int, ffmpeg: str) -> list[str]:
    """Build the ffmpeg argument vector that normalises ``src`` to a canvas.

    Pure: returns the argument list, executes nothing, and does not include an
    output path - the caller appends one, since encoding to a specific target
    file is the caller's concern, not this function's.

    Scale-and-pad, never crop-or-stretch: ``force_original_aspect_ratio=decrease``
    fits the source inside the target box without distortion, and the ``pad``
    stage centres it on a solid canvas of exactly ``width``x``height``. A
    stretched clip is instantly visible to a viewer, which is why cropping or
    stretching are both refused in favour of this.

    CFR (``-r 30``) and pixel format (``-pix_fmt yuv420p``) are pinned so every
    normalised clip in the library is stream-copy-compatible with every other
    one downstream, regardless of the source's own frame rate or pixel format.
    ``-an`` drops the source audio track - narration is the only audio track
    this pipeline ever mixes in, and a stray B-roll track would double up
    under the mux. ``-c:v libx264`` is pinned explicitly rather than left to
    ffmpeg's container default, so every normalised clip in the library is the
    same well-understood, universally-decodable codec regardless of which
    machine ingested it.
    """
    vf = (
        f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
        f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2"
    )
    return [
        ffmpeg,
        "-hide_banner",
        "-y",
        "-i",
        str(src),
        "-vf",
        vf,
        "-r",
        _TARGET_FPS,
        "-pix_fmt",
        _TARGET_PIX_FMT,
        "-an",
        "-c:v",
        "libx264",
    ]


def _run_normalise(src: Path, dest: Path, *, width: int, height: int, ffmpeg: str) -> None:
    """Execute ``normalise_clip``'s argument vector against a real output path.

    ``subprocess.run`` with captured output, not a manual ``Popen`` - this
    project has already shipped one leaked-pipe bug, and ``ResourceWarning``/
    ``PytestUnraisableExceptionWarning`` are promoted to errors specifically to
    catch it happening again.

    Raises:
        RenderError: ffmpeg exited non-zero.
        subprocess.TimeoutExpired: the encode did not finish within the
            timeout.
        OSError: ``ffmpeg`` cannot be executed.
    """
    args = [*normalise_clip(src, width=width, height=height, ffmpeg=ffmpeg), str(dest)]
    result = subprocess.run(
        args,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=_ENCODE_TIMEOUT_S,
        check=False,
    )
    if result.returncode != 0:
        raise RenderError(
            f"ffmpeg exited {result.returncode} normalising {src} to {width}x{height}: "
            f"{result.stderr.strip()}"
        )


class BrollLibrary:
    """CRUD-and-ingest over the ``broll_clips`` table of a migrated connection."""

    def __init__(self, conn: sqlite3.Connection, cas: CasStore) -> None:
        self._conn = conn
        self._cas = cas

    def add(
        self,
        path: Path,
        source_url: str,
        licence: str,
        attribution: str = "",
        notes: str = "",
    ) -> str:
        """Ingest ``path`` as a new B-roll clip: probe, normalise, record.

        Probes the source once for its width/height/duration, stores the
        original untouched (copied, never moved - ``path`` belongs to the
        caller), then runs ``normalise_clip`` twice - once per canvas, both
        transcoded from the original source, never one cropped from the other
        - and stores both results. The row insert (source digest, both
        normalised digests, duration, source dimensions, and every provenance
        field) happens last, inside a single ``transaction(conn,
        immediate=True)``, so a crash or ffmpeg failure partway through never
        leaves a half-written row. All three digests are ``retain()``-ed
        inside that same transaction: the row and its protection against the
        CAS evictor commit atomically or not at all - the established pattern
        for every persistent row that references a digest (see
        ``dispatcher.py``'s job-pin retain, alongside its own note that an
        un-retained digest is a silent, permanent-until-eviction leak the
        moment the evictor gets a production caller). Without this, every
        ingested clip's digests sit at ``refcount = 0`` and are eligible for
        deletion under disk pressure while ``broll_clips`` rows and the
        manifest still point at them - exactly the DMCA provenance record
        this task exists to make durable.

        Refuses a source whose content already has a ``broll_clips`` row,
        identified by hashing ``path`` *before* any transcoding is attempted -
        hashing is cheap, two ffmpeg encodes are not, and a duplicate add
        should cost the caller a second, not a minute. This is a hard refusal,
        not a silent no-op: the caller explicitly asked to add something, so
        failing loudly and naming the clip that already holds this footage is
        the least surprising behaviour. It is also what keeps "one clip, one
        row" true - without it, a second `add()` of the same file dedupes
        every digest in the CAS (no new blob) but still inserts a second row
        under a new ``clip_id``, so ``write_manifest`` would emit two entries
        for what is physically one clip, silently skewing selection
        probability and risking the same footage appearing twice in one
        video.

        Raises:
            ValidationError: ``source_url`` or ``licence`` is blank, ``path``
                does not exist, ``path``'s content is already recorded under
                an existing ``clip_id``, or ffprobe could not determine the
                source's dimensions or duration.
            RenderError: either normalisation encode exited non-zero.
            subprocess.TimeoutExpired: an encode did not finish within the
                timeout.
            OSError: ffmpeg/ffprobe cannot be executed, or a filesystem
                operation fails.
            sqlite3.OperationalError: the write lock could not be acquired
                within ``busy_timeout``.
        """
        # `not source_url.strip()` alone raises AttributeError instead of
        # ValidationError if a caller ever passes None despite the str
        # annotation - the short-circuit keeps this a validation error, not a
        # crash, matching the emptiness check it already promises to make.
        if not source_url or not source_url.strip():
            raise ValidationError("source_url must not be blank")
        if not licence or not licence.strip():
            raise ValidationError("licence must not be blank")

        if not path.is_file():
            raise ValidationError(f"source file does not exist: {path}")

        # Dedup check, before locate()/probe_media()/any transcode: hash_file
        # is a plain streaming read, cheap next to a subprocess round trip and
        # nowhere near the cost of two ffmpeg encodes. CasStore.put_file below
        # recomputes this same digest when it stores the source - a second
        # read of a B-roll-sized file is a deliberate, sanctioned trade-off
        # against restructuring the CAS write to return a digest without one.
        existing = self._conn.execute(
            "SELECT id FROM broll_clips WHERE source_digest = ?", (hash_file(path),)
        ).fetchone()
        if existing is not None:
            raise ValidationError(
                f"{path} is already in the B-roll library as clip {existing['id']!r} "
                "- refusing to add the same footage under a second clip_id"
            )

        binaries = locate()
        info = probe_media(path, ffprobe=binaries.ffprobe)

        source_digest = self._cas.put_file(path, kind=_VIDEO_KIND)

        with tempfile.TemporaryDirectory(prefix="ytauto-broll-") as tmp:
            tmp_dir = Path(tmp)
            landscape_out = tmp_dir / "landscape.mp4"
            vertical_out = tmp_dir / "vertical.mp4"

            _run_normalise(
                path,
                landscape_out,
                width=LANDSCAPE[0],
                height=LANDSCAPE[1],
                ffmpeg=str(binaries.ffmpeg),
            )
            _run_normalise(
                path,
                vertical_out,
                width=VERTICAL[0],
                height=VERTICAL[1],
                ffmpeg=str(binaries.ffmpeg),
            )

            landscape_digest = self._cas.put_file(landscape_out, kind=_VIDEO_KIND, move=True)
            vertical_digest = self._cas.put_file(vertical_out, kind=_VIDEO_KIND, move=True)

        clip_id = uuid.uuid4().hex
        now = utc_now_iso()
        with transaction(self._conn, immediate=True):
            self._conn.execute(
                """
                INSERT INTO broll_clips
                    (id, source_digest, normalised_landscape_digest,
                     normalised_vertical_digest, duration_s, width, height,
                     source_url, licence, attribution, notes, added_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    clip_id,
                    source_digest,
                    landscape_digest,
                    vertical_digest,
                    info.duration_s,
                    info.width,
                    info.height,
                    source_url,
                    licence,
                    attribution,
                    notes,
                    now,
                ),
            )
            # Pin all three against the evictor in the same transaction as the
            # row that references them - see the docstring above. Each digest
            # already has a cas_objects row (put_file/record_blob wrote it
            # earlier), so retain() only ever increments refcount here.
            for digest in (source_digest, landscape_digest, vertical_digest):
                self._cas.retain(digest)
        return clip_id

    def write_manifest(self) -> ContentHash:
        """Rewrite the CAS-blob manifest: one entry per clip currently in the library.

        The entry shape is a public contract - Task 10's clip selection and
        Tasks 11-12's compose stages all read it:
        ``{"clip_id", "duration_s", "source_width", "source_height",
        "normalised_landscape_digest", "normalised_vertical_digest"}``.
        ``segments.json`` references ``clip_id`` rather than a digest directly,
        which is what lets one selection serve both canvases.

        Ordered by ``added_at`` so the manifest is deterministic across
        rewrites for an unchanged library, rather than depending on SQLite's
        unspecified row order - with ``id`` as a secondary key, since two
        clips added within the same ``utc_now_iso()`` tick would otherwise
        order arbitrarily between rewrites, changing the manifest's bytes (and
        so its digest) with no change to the library at all. That would
        silently ripple into a spurious cache miss wherever the manifest
        digest feeds a fingerprint downstream (e.g. ``select_broll``'s), with
        no visible cause.

        **The manifest is retained, and the manifest it replaces released.**
        Found by the whole-branch review, and the same bug class as ``add``'s
        missing clip retains: ``put_bytes`` records the row at ``refcount =
        0`` and stops. The manifest is not a stage artifact, so
        ``commit_stage``'s retain never sees it; nothing calls ``touch()`` on
        it either, so its ``last_accessed_at`` never advances past the moment
        it was written and it sorts *first* in ``iter_evictable``'s LRU order.
        The moment ``Evictor.run()`` gets a production caller, the very first
        blob deleted would be the one ``select_broll`` and both compose
        stages all read. Retaining removes it from ``iter_evictable``
        entirely, which is the stronger fix than merely touching it.

        Exactly one manifest is pinned at a time. Every manifest currently
        pinned is read *inside* the write transaction, the new one is
        retained, and each of those previous pins is then released - so a
        rewrite that changes the manifest hands the pin over atomically, and
        a rewrite that produces byte-identical output (an unchanged library,
        which is every ``ytauto run`` against a library nobody has touched)
        nets out to no change at all rather than ratcheting the refcount up
        on every invocation. The old manifest drops to zero and becomes
        ordinarily evictable, which is correct: nothing reads it once
        ``refresh_run_settings`` has rebound ``broll_manifest_digest``.

        Raises:
            ValidationError: a previously-pinned manifest digest names no
                stored object (from ``CasStore.release``).
            sqlite3.Error: the query fails.
            sqlite3.OperationalError: the write lock could not be acquired
                within ``busy_timeout``.
            OSError: the manifest cannot be staged into the CAS.
        """
        rows = self._conn.execute(
            """
            SELECT id, duration_s, width, height,
                   normalised_landscape_digest, normalised_vertical_digest
            FROM broll_clips
            ORDER BY added_at ASC, id ASC
            """
        ).fetchall()
        entries = [
            {
                "clip_id": row["id"],
                "duration_s": row["duration_s"],
                "source_width": row["width"],
                "source_height": row["height"],
                "normalised_landscape_digest": row["normalised_landscape_digest"],
                "normalised_vertical_digest": row["normalised_vertical_digest"],
            }
            for row in rows
        ]
        payload = json.dumps(entries, indent=2).encode("utf-8")
        with transaction(self._conn, immediate=True):
            # Read before the retain below, so a rewrite producing the same
            # bytes finds its own existing pin here and nets out to no change.
            previously_pinned = [
                ContentHash(row["hash"])
                for row in self._conn.execute(
                    "SELECT hash FROM cas_objects WHERE kind = ? AND refcount > 0",
                    (_MANIFEST_KIND,),
                ).fetchall()
            ]
            digest = self._cas.put_bytes(payload, kind=_MANIFEST_KIND)
            self._cas.retain(digest)
            for previous in previously_pinned:
                self._cas.release(previous)
        return digest
