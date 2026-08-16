"""Unit tests for ``infra.broll``.

``normalise_clip`` is pure and tested directly. ``BrollLibrary.add`` shells
out to ffmpeg/ffprobe, so ``locate``, ``probe_media`` and the ffmpeg subprocess
call are all monkeypatched here to keep the unit suite hermetic; the real
binary is exercised by ``tests/integration/test_broll_ingest.py``.

``db_conn`` is defined in ``tests/unit/conftest.py``.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
from pathlib import Path

import pytest

from ytauto.core.errors import RenderError, ValidationError
from ytauto.infra.broll import BrollLibrary, _run_normalise, normalise_clip
from ytauto.infra.cas.store import CasStore, hash_file
from ytauto.infra.db.engine import transaction
from ytauto.infra.ffmpeg.locator import FfmpegBinaries
from ytauto.infra.ffmpeg.media_probe import MediaInfo

# -- normalise_clip: pure argument construction --------------------------


def test_normalisation_scales_and_pads_rather_than_stretching() -> None:
    """A stretched clip is instantly visible; aspect must be preserved."""
    args = normalise_clip(Path("in.mp4"), width=1080, height=1920, ffmpeg="ffmpeg")
    vf = args[args.index("-vf") + 1]
    assert "force_original_aspect_ratio=decrease" in vf
    assert "pad=1080:1920" in vf


def test_normalisation_drops_the_source_audio() -> None:
    assert "-an" in normalise_clip(Path("in.mp4"), width=1920, height=1080, ffmpeg="ffmpeg")


def test_normalisation_pins_cfr_and_pixel_format_for_stream_copy() -> None:
    args = normalise_clip(Path("in.mp4"), width=1920, height=1080, ffmpeg="ffmpeg")
    assert args[args.index("-r") + 1] == "30"
    assert args[args.index("-pix_fmt") + 1] == "yuv420p"


def test_normalisation_never_crops() -> None:
    """Mutation guard: 'crop' must never appear in the filter chain - only
    scale-then-pad, never a crop filter that would discard picture content."""
    args = normalise_clip(Path("in.mp4"), width=1080, height=1920, ffmpeg="ffmpeg")
    vf = args[args.index("-vf") + 1]
    assert "crop" not in vf


def test_normalisation_returns_no_output_path() -> None:
    """The output path is the caller's concern; normalise_clip is pure and
    knows nothing about where the result is written."""
    args = normalise_clip(Path("in.mp4"), width=1920, height=1080, ffmpeg="ffmpeg")
    assert args[args.index("-i") + 1] == "in.mp4"
    assert not any(arg.endswith(".mp4") and arg != "in.mp4" for arg in args)


# -- _run_normalise: the ffmpeg subprocess boundary itself ---------------


def test_run_normalise_raises_render_error_on_a_nonzero_ffmpeg_exit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Exercises _run_normalise's own returncode check - the RenderError test
    below mocks this function out entirely, so it never touches this guard."""

    def _fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="unknown encoder 'libx264'"
        )

    monkeypatch.setattr("ytauto.infra.broll.subprocess.run", _fake_run)

    with pytest.raises(RenderError, match="unknown encoder"):
        _run_normalise(
            tmp_path / "in.mp4", tmp_path / "out.mp4", width=1920, height=1080, ffmpeg="ffmpeg"
        )


# -- BrollLibrary.add: DB + provenance, ffmpeg/ffprobe fully mocked ------


@pytest.fixture()
def cas(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    return CasStore(root=tmp_path / "cas", conn=db_conn)


@pytest.fixture()
def source_file(tmp_path: Path) -> Path:
    src = tmp_path / "source.mp4"
    src.write_bytes(b"source video bytes")
    return src


def _patch_ffmpeg(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Fake `locate`, `probe_media`, and the ffmpeg subprocess call.

    The fake encode writes a distinct placeholder file at the requested
    output path (mirroring what real ffmpeg would do) so `CasStore.put_file`
    has something real to hash and store.
    """

    def _fake_locate(*_a: object, **_k: object) -> FfmpegBinaries:
        return FfmpegBinaries(
            ffmpeg=tmp_path / "ffmpeg.exe", ffprobe=tmp_path / "ffprobe.exe", version="7.1.1"
        )

    def _fake_probe_media(path: Path, *, ffprobe: Path) -> MediaInfo:
        return MediaInfo(width=3840, height=2160, duration_s=12.5)

    def _fake_run_normalise(src: Path, dest: Path, *, width: int, height: int, ffmpeg: str) -> None:
        dest.write_bytes(f"normalised {width}x{height} of {src.name}".encode())

    monkeypatch.setattr("ytauto.infra.broll.locate", _fake_locate)
    monkeypatch.setattr("ytauto.infra.broll.probe_media", _fake_probe_media)
    monkeypatch.setattr("ytauto.infra.broll._run_normalise", _fake_run_normalise)


def test_add_inserts_a_row_with_both_digests_and_the_probed_source_metadata(
    db_conn: sqlite3.Connection,
    cas: CasStore,
    source_file: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ffmpeg(monkeypatch, tmp_path)

    clip_id = BrollLibrary(db_conn, cas).add(
        source_file,
        source_url="https://example.com/clip",
        licence="CC0",
        attribution="Jane Doe",
        notes="stock footage",
    )

    row = db_conn.execute("SELECT * FROM broll_clips WHERE id = ?", (clip_id,)).fetchone()
    assert row is not None
    assert row["duration_s"] == 12.5
    assert row["width"] == 3840
    assert row["height"] == 2160
    assert row["source_url"] == "https://example.com/clip"
    assert row["licence"] == "CC0"
    assert row["attribution"] == "Jane Doe"
    assert row["notes"] == "stock footage"
    assert cas.exists(row["source_digest"])
    assert cas.exists(row["normalised_landscape_digest"])
    assert cas.exists(row["normalised_vertical_digest"])
    assert row["normalised_landscape_digest"] != row["normalised_vertical_digest"]


def test_add_retains_all_three_digests_against_the_evictor(
    db_conn: sqlite3.Connection,
    cas: CasStore,
    source_file: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row referencing a digest sitting at refcount=0 is a silent-deletion
    trap the moment the evictor gets a production caller (it has none today -
    see dispatcher.py's own retain, whose comment warns about exactly this).
    Every digest a broll_clips row references must be retained in the same
    transaction as the insert."""
    _patch_ffmpeg(monkeypatch, tmp_path)

    clip_id = BrollLibrary(db_conn, cas).add(source_file, source_url="local", licence="CC0")

    row = db_conn.execute("SELECT * FROM broll_clips WHERE id = ?", (clip_id,)).fetchone()
    for digest in (
        row["source_digest"],
        row["normalised_landscape_digest"],
        row["normalised_vertical_digest"],
    ):
        assert cas.refcount(digest) == 1


def test_add_stores_the_original_source_by_copy_not_move(
    db_conn: sqlite3.Connection,
    cas: CasStore,
    source_file: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The source path belongs to the caller; ingest must never delete it."""
    _patch_ffmpeg(monkeypatch, tmp_path)

    BrollLibrary(db_conn, cas).add(source_file, source_url="local", licence="CC0")

    assert source_file.exists()


def test_add_rejects_a_blank_licence(
    db_conn: sqlite3.Connection,
    cas: CasStore,
    source_file: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ffmpeg(monkeypatch, tmp_path)

    with pytest.raises(ValidationError, match="licence"):
        BrollLibrary(db_conn, cas).add(source_file, source_url="local", licence="   ")

    assert db_conn.execute("SELECT count(*) AS n FROM broll_clips").fetchone()["n"] == 0


def test_add_rejects_a_blank_source_url(
    db_conn: sqlite3.Connection,
    cas: CasStore,
    source_file: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ffmpeg(monkeypatch, tmp_path)

    with pytest.raises(ValidationError, match="source_url"):
        BrollLibrary(db_conn, cas).add(source_file, source_url="", licence="CC0")

    assert db_conn.execute("SELECT count(*) AS n FROM broll_clips").fetchone()["n"] == 0


def test_add_rejects_a_none_licence_as_a_validation_error_not_a_crash(
    db_conn: sqlite3.Connection,
    cas: CasStore,
    source_file: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller violating the str annotation must still get ValidationError,
    not an AttributeError from calling .strip() on None."""
    _patch_ffmpeg(monkeypatch, tmp_path)

    with pytest.raises(ValidationError, match="licence"):
        BrollLibrary(db_conn, cas).add(source_file, source_url="local", licence=None)  # type: ignore[arg-type]


def test_add_rejects_a_none_source_url_as_a_validation_error_not_a_crash(
    db_conn: sqlite3.Connection,
    cas: CasStore,
    source_file: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ffmpeg(monkeypatch, tmp_path)

    with pytest.raises(ValidationError, match="source_url"):
        BrollLibrary(db_conn, cas).add(source_file, source_url=None, licence="CC0")  # type: ignore[arg-type]


def test_add_raises_render_error_when_ffmpeg_fails(
    db_conn: sqlite3.Connection,
    cas: CasStore,
    source_file: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mutation guard: a failed encode must surface as RenderError, not be
    swallowed or reported as a successful add."""
    _patch_ffmpeg(monkeypatch, tmp_path)

    def _boom(src: Path, dest: Path, *, width: int, height: int, ffmpeg: str) -> None:
        raise RenderError(f"ffmpeg exited 1 normalising {src} to {width}x{height}: boom")

    monkeypatch.setattr("ytauto.infra.broll._run_normalise", _boom)

    with pytest.raises(RenderError, match="boom"):
        BrollLibrary(db_conn, cas).add(source_file, source_url="local", licence="CC0")

    assert db_conn.execute("SELECT count(*) AS n FROM broll_clips").fetchone()["n"] == 0
    # The source was already staged before the second transcode failed. It is
    # correctly left un-retained at refcount=0 - an orphan the evictor is free
    # to reclaim, not a leak to clean up. No row references it, so there is
    # nothing for a retain to protect.
    source_digest = hash_file(source_file)
    assert cas.exists(source_digest)
    assert cas.refcount(source_digest) == 0


def test_add_reports_the_probe_failure_when_duration_cannot_be_determined(
    db_conn: sqlite3.Connection,
    cas: CasStore,
    source_file: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A source ffprobe cannot fully describe must fail before any encode runs,
    not silently proceed with a zero duration."""
    _patch_ffmpeg(monkeypatch, tmp_path)

    def _boom_probe(path: Path, *, ffprobe: Path) -> MediaInfo:
        raise ValidationError(f"could not determine a positive duration for {path}")

    monkeypatch.setattr("ytauto.infra.broll.probe_media", _boom_probe)

    with pytest.raises(ValidationError, match="duration"):
        BrollLibrary(db_conn, cas).add(source_file, source_url="local", licence="CC0")

    assert db_conn.execute("SELECT count(*) AS n FROM broll_clips").fetchone()["n"] == 0


# -- write_manifest --------------------------------------------------------


def test_write_manifest_is_empty_for_an_empty_library(
    db_conn: sqlite3.Connection, cas: CasStore
) -> None:
    digest = BrollLibrary(db_conn, cas).write_manifest()
    assert cas.read_bytes(digest).decode("utf-8").strip() == "[]"


def test_write_manifest_entry_shape_matches_the_documented_contract(
    db_conn: sqlite3.Connection,
    cas: CasStore,
    source_file: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task 10 and Tasks 11-12 both read this shape; the key set is a contract."""
    _patch_ffmpeg(monkeypatch, tmp_path)
    library = BrollLibrary(db_conn, cas)
    clip_id = library.add(source_file, source_url="local", licence="CC0")

    digest = library.write_manifest()
    entries = json.loads(cas.read_bytes(digest))

    assert len(entries) == 1
    entry = entries[0]
    assert set(entry) == {
        "clip_id",
        "duration_s",
        "source_width",
        "source_height",
        "normalised_landscape_digest",
        "normalised_vertical_digest",
    }
    assert entry["clip_id"] == clip_id
    assert entry["duration_s"] == 12.5
    assert entry["source_width"] == 3840
    assert entry["source_height"] == 2160


def test_write_manifest_rewrites_to_reflect_every_add_since_the_last_call(
    db_conn: sqlite3.Connection,
    cas: CasStore,
    source_file: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ffmpeg(monkeypatch, tmp_path)
    library = BrollLibrary(db_conn, cas)

    first_digest = library.write_manifest()
    library.add(source_file, source_url="local", licence="CC0")
    second_digest = library.write_manifest()

    assert first_digest != second_digest
    entries = json.loads(cas.read_bytes(second_digest))
    assert len(entries) == 1


def test_write_manifest_breaks_added_at_ties_by_id_for_a_stable_digest(
    db_conn: sqlite3.Connection, cas: CasStore
) -> None:
    """Two clips sharing one utc_now_iso() tick must still order the same way
    on every rewrite - otherwise the manifest's bytes (and so its digest, and
    so any fingerprint downstream that reads it) change with no change to the
    library at all. Rows are inserted directly, in reverse-id order, so a
    query with no tiebreaker would be the one case in this suite likely to
    expose SQLite's actual (unspecified, often insertion-order) row order."""
    same_timestamp = "2026-01-01T00:00:00+00:00"
    with transaction(db_conn, immediate=True):
        for clip_id in ("zzz-later-id", "aaa-earlier-id"):
            db_conn.execute(
                """
                INSERT INTO broll_clips
                    (id, source_digest, normalised_landscape_digest,
                     normalised_vertical_digest, duration_s, width, height,
                     source_url, licence, attribution, notes, added_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    clip_id,
                    "0" * 64,
                    "1" * 64,
                    "2" * 64,
                    1.0,
                    640,
                    480,
                    "local",
                    "CC0",
                    "",
                    "",
                    same_timestamp,
                ),
            )

    digest = BrollLibrary(db_conn, cas).write_manifest()
    entries = json.loads(cas.read_bytes(digest))

    assert [e["clip_id"] for e in entries] == ["aaa-earlier-id", "zzz-later-id"]
