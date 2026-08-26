"""Unit tests for ``infra.music``.

``MusicLibrary.add`` shells out to ffprobe, so the tests here cover the
branches that run *before* it does - the provenance guards and the duplicate
refusal, all of which short-circuit ahead of ``locate()``. The full ingest
against a real binary is ``tests/integration``'s job, in the same split
``test_broll.py`` uses.

``db_conn`` is defined in ``tests/unit/conftest.py``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ytauto.core.errors import ValidationError
from ytauto.infra.cas.store import CasStore, hash_file
from ytauto.infra.db.engine import transaction
from ytauto.infra.music import MusicLibrary


def _library(tmp_path: Path, conn: sqlite3.Connection) -> MusicLibrary:
    return MusicLibrary(conn, CasStore(root=tmp_path / "cas", conn=conn))


def _track_file(tmp_path: Path, name: str = "bed.mp3", body: bytes = b"not really audio") -> Path:
    path = tmp_path / name
    path.write_bytes(body)
    return path


# -- the provenance record is the point ---------------------------------------


def test_a_track_with_no_source_url_is_refused(tmp_path: Path, db_conn: sqlite3.Connection) -> None:
    """Music is the most Content-ID-matched thing on YouTube: a bed with no
    recorded origin is exactly the row this table must not hold."""
    with pytest.raises(ValidationError, match="source_url"):
        _library(tmp_path, db_conn).add(_track_file(tmp_path), source_url="   ", licence="CC0")


def test_a_track_with_no_licence_is_refused(tmp_path: Path, db_conn: sqlite3.Connection) -> None:
    with pytest.raises(ValidationError, match="licence"):
        _library(tmp_path, db_conn).add(
            _track_file(tmp_path), source_url="https://example.com/t", licence=""
        )


def test_the_guards_run_before_any_subprocess(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Not merely that it raises, but that it raises without paying for
    ffprobe - and, more importantly, that a missing licence can never be
    reported as some downstream ffprobe failure instead."""

    def _explode() -> None:
        raise AssertionError("locate() must not be reached for invalid provenance")

    monkeypatch.setattr("ytauto.infra.music.locate", _explode)
    with pytest.raises(ValidationError):
        _library(tmp_path, db_conn).add(_track_file(tmp_path), source_url="", licence="")


def test_a_missing_file_is_refused_by_name(tmp_path: Path, db_conn: sqlite3.Connection) -> None:
    with pytest.raises(ValidationError, match="does not exist"):
        _library(tmp_path, db_conn).add(
            tmp_path / "nope.mp3", source_url="https://example.com/t", licence="CC0"
        )


# -- one track, one row --------------------------------------------------------


def test_the_same_audio_cannot_be_added_twice(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two rows for one file would put the same bed in the picker twice under
    different ids, and a project would name one of them arbitrarily."""
    path = _track_file(tmp_path)
    with transaction(db_conn, immediate=True):
        db_conn.execute(
            """
            INSERT INTO music_tracks (id, source_digest, duration_s, title,
                                      source_url, licence, attribution, notes, added_at)
            VALUES (?, ?, ?, ?, ?, ?, '', '', ?)
            """,
            (
                "existing",
                hash_file(path),
                12.5,
                "Bed",
                "https://example.com/t",
                "CC0",
                "2026-01-01T00:00:00Z",
            ),
        )

    def _explode() -> None:
        raise AssertionError("the duplicate must be caught before ffprobe runs")

    monkeypatch.setattr("ytauto.infra.music.locate", _explode)
    with pytest.raises(ValidationError, match="already in the music library"):
        _library(tmp_path, db_conn).add(path, source_url="https://example.com/t", licence="CC0")


# -- lookups the compose path depends on ---------------------------------------


def test_an_unknown_track_id_resolves_to_none(tmp_path: Path, db_conn: sqlite3.Connection) -> None:
    """``refresh_run_settings`` turns this ``None`` into a clear message at
    enqueue time rather than a failure three stages into a render."""
    assert _library(tmp_path, db_conn).digest_for("no-such-track") is None
