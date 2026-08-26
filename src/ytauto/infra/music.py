"""Ingest and record music beds, with their licence provenance.

Shaped deliberately like ``infra.broll``, because the two solve the same
problem - a global, mutable library of third-party media that a render reads
from, where the provenance record is the point - but with one structural
difference and one raised stake.

**The difference: no normalisation.** ``BrollLibrary.add`` transcodes every
clip twice, once per canvas, because a 1920x1080 master and a 1080x1920
master need genuinely different pixels. Audio has no such split: one file is
mixed unchanged under both canvases, so there is a single ``source_digest``
here where B-roll carries three digests. The source is stored untouched and
ffmpeg decodes whatever it is at compose time.

**The stake: music is what gets claimed.** The B-roll library records licence
and source because unattributed footage is a DMCA risk. For music that risk
is not comparable - it is the single most Content-ID-matched category on
YouTube, matching is automated and runs on every upload, and a match on the
*bed* claims the whole video regardless of how impeccably the footage under
it is licensed. So ``source_url`` and ``licence`` are required here for the
same reason they are required there, only with less room for argument.
"""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from ytauto.core.errors import ValidationError
from ytauto.core.models.content_hash import hash_file
from ytauto.infra.cas.store import CasStore, ContentHash
from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import transaction
from ytauto.infra.ffmpeg.locator import locate
from ytauto.infra.ffmpeg.media_probe import probe_audio_duration

_AUDIO_KIND = "audio"


@dataclass(frozen=True)
class MusicTrack:
    """One row of ``music_tracks``, as the UI and CLI want to read it."""

    id: str
    source_digest: str
    duration_s: float
    title: str
    source_url: str
    licence: str
    attribution: str
    notes: str
    added_at: str


class MusicLibrary:
    """CRUD-and-ingest over the ``music_tracks`` table of a migrated connection."""

    def __init__(self, conn: sqlite3.Connection, cas: CasStore) -> None:
        self._conn = conn
        self._cas = cas

    def add(
        self,
        path: Path,
        source_url: str,
        licence: str,
        title: str = "",
        attribution: str = "",
        notes: str = "",
    ) -> str:
        """Ingest ``path`` as a new music track: probe, store, record.

        The row insert and the digest ``retain()`` happen together inside one
        ``transaction(conn, immediate=True)``, for the reason
        ``BrollLibrary.add`` spells out at length: an un-retained digest is
        eligible for eviction under disk pressure while a row still points at
        it, which would quietly destroy the provenance record this table
        exists to keep.

        Refuses a source whose content already has a row, identified by
        hashing before anything else happens. Unlike B-roll there is no
        expensive transcode to protect here - the refusal is about keeping
        "one track, one row" true, so a bed cannot appear twice in a picker
        under two ids.

        Raises:
            ValidationError: ``source_url`` or ``licence`` is blank, ``path``
                does not exist, ``path``'s content is already recorded, or
                ffprobe found no audio stream or no positive duration.
            subprocess.TimeoutExpired: ffprobe did not finish within 30s.
            OSError: ffprobe cannot be executed, or a filesystem operation
                fails.
            sqlite3.OperationalError: the write lock could not be acquired
                within ``busy_timeout``.
        """
        if not source_url or not source_url.strip():
            raise ValidationError("source_url must not be blank")
        if not licence or not licence.strip():
            raise ValidationError("licence must not be blank")
        if not path.is_file():
            raise ValidationError(f"source file does not exist: {path}")

        existing = self._conn.execute(
            "SELECT id FROM music_tracks WHERE source_digest = ?", (hash_file(path),)
        ).fetchone()
        if existing is not None:
            raise ValidationError(
                f"{path} is already in the music library as track {existing['id']!r} "
                "- refusing to add the same audio under a second track id"
            )

        binaries = locate()
        duration_s = probe_audio_duration(path, ffprobe=binaries.ffprobe)

        source_digest = self._cas.put_file(path, kind=_AUDIO_KIND)
        track_id = uuid.uuid4().hex

        with transaction(self._conn, immediate=True):
            self._cas.retain(source_digest)
            self._conn.execute(
                """
                INSERT INTO music_tracks (
                    id, source_digest, duration_s, title,
                    source_url, licence, attribution, notes, added_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    track_id,
                    source_digest,
                    float(duration_s),
                    title.strip() or path.stem,
                    source_url.strip(),
                    licence.strip(),
                    attribution.strip(),
                    notes.strip(),
                    utc_now_iso(),
                ),
            )
        return track_id

    def list_tracks(self) -> Sequence[MusicTrack]:
        """Every track, newest first - what the picker and the library page read."""
        rows = self._conn.execute(
            """
            SELECT id, source_digest, duration_s, title, source_url,
                   licence, attribution, notes, added_at
            FROM music_tracks
            ORDER BY added_at DESC, id DESC
            """
        ).fetchall()
        return [
            MusicTrack(
                id=row["id"],
                source_digest=row["source_digest"],
                duration_s=float(row["duration_s"]),
                title=row["title"],
                source_url=row["source_url"],
                licence=row["licence"],
                attribution=row["attribution"],
                notes=row["notes"],
                added_at=row["added_at"],
            )
            for row in rows
        ]

    def digest_for(self, track_id: str) -> ContentHash | None:
        """The CAS digest of ``track_id``'s audio, or ``None`` if no such row.

        ``None`` rather than an exception because the caller that matters -
        ``ComposeStage`` - has to decide what an unknown id means in its own
        terms (a track deleted after a project selected it), and a bare
        lookup miss is not by itself an error here.
        """
        row = self._conn.execute(
            "SELECT source_digest FROM music_tracks WHERE id = ?", (track_id,)
        ).fetchone()
        return ContentHash(row["source_digest"]) if row is not None else None
