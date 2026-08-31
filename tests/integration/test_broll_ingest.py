"""Integration test: B-roll ingest against a real ffmpeg/ffprobe.

Deliberately uses a 640x480 source - neither canvas's aspect ratio - so a
single test proves scale-and-pad works in *both* directions: 640x480 pads to
1920x1080 with vertical bars trimmed (letterboxed by height), and pads to
1080x1920 with horizontal bars (pillarboxed by width). A source that already
matched one canvas would leave that direction unexercised.

``db_conn``/``cas`` are defined locally rather than added to
``tests/integration/conftest.py`` - the same choice ``test_resume.py`` makes,
for the same reason: this suite's fixtures are not needed by the
dispatcher-driving tests that live in that shared conftest.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

from ytauto.infra.broll import LANDSCAPE, VERTICAL, BrollLibrary
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations
from ytauto.infra.ffmpeg.locator import FfmpegBinaries, locate
from ytauto.infra.ffmpeg.media_probe import probe_dimensions

pytestmark = pytest.mark.integration


def _lavfi_clip(tmp_path: Path, source_filter: str, seconds: float) -> Path:
    """A deterministic synthetic clip, generated fresh rather than committed.

    Never commit a video file to the repo - this is what the brief for this
    task calls out explicitly. ``testsrc2`` is ffmpeg's own built-in pattern
    generator, so this needs nothing beyond the ffmpeg binary already required
    for the rest of the suite.
    """
    binaries = locate()
    out = tmp_path / "source.mp4"
    result = subprocess.run(
        [
            str(binaries.ffmpeg),
            "-hide_banner",
            "-y",
            "-f",
            "lavfi",
            "-i",
            source_filter,
            "-t",
            str(seconds),
            "-an",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
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


def test_a_source_clip_is_normalised_to_both_canvases(
    tmp_path: Path, cas: CasStore, db_conn: sqlite3.Connection, ffmpeg_binaries: FfmpegBinaries
) -> None:
    src = _lavfi_clip(tmp_path, "testsrc2=size=640x480:rate=25", seconds=2)

    clip_id = BrollLibrary(db_conn, cas).add(
        src, source_url="local", licence="CC0", attribution="", notes=""
    )

    row = db_conn.execute("SELECT * FROM broll_clips WHERE id = ?", (clip_id,)).fetchone()
    assert row["width"] == 640
    assert row["height"] == 480
    assert row["duration_s"] == pytest.approx(2.0, abs=0.1)

    for digest, expected in (
        (row["normalised_landscape_digest"], LANDSCAPE),
        (row["normalised_vertical_digest"], VERTICAL),
    ):
        assert probe_dimensions(cas.path_for(digest), ffprobe=ffmpeg_binaries.ffprobe) == expected


def test_write_manifest_after_add_produces_a_reloadable_cas_blob(
    tmp_path: Path, cas: CasStore, db_conn: sqlite3.Connection
) -> None:
    src = _lavfi_clip(tmp_path, "testsrc2=size=640x480:rate=25", seconds=1)
    library = BrollLibrary(db_conn, cas)
    clip_id = library.add(src, source_url="local", licence="CC0")

    digest = library.write_manifest()

    entries = json.loads(cas.read_bytes(digest))
    assert len(entries) == 1
    assert entries[0]["clip_id"] == clip_id
