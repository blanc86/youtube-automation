"""CLI wiring for ``ytauto broll add``.

Only the wiring is tested here: that --source-url/--licence are genuinely
required, and that a successful invocation calls through to
``BrollLibrary.add``/``write_manifest``. ``BrollLibrary`` itself is covered by
``tests/unit/infra/test_broll.py``; the real ffmpeg/ffprobe path is covered by
``tests/integration/test_broll_ingest.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ytauto.cli.__main__ import main


@pytest.fixture()
def source_file(tmp_path: Path) -> Path:
    src = tmp_path / "clip.mp4"
    src.write_bytes(b"not real video")
    return src


def test_source_url_is_required(
    tmp_path: Path, source_file: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--data-dir",
                str(tmp_path),
                "broll",
                "add",
                str(source_file),
                "--licence",
                "CC0",
            ]
        )
    assert exc_info.value.code == 2
    assert "--source-url" in capsys.readouterr().err


def test_licence_is_required(
    tmp_path: Path, source_file: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--data-dir",
                str(tmp_path),
                "broll",
                "add",
                str(source_file),
                "--source-url",
                "local",
            ]
        )
    assert exc_info.value.code == 2
    assert "--licence" in capsys.readouterr().err


def test_a_successful_add_calls_through_to_the_library_and_rewrites_the_manifest(
    tmp_path: Path, source_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: dict[str, object] = {}

    class _FakeLibrary:
        def __init__(self, conn: object, cas: object) -> None:
            pass

        def add(
            self,
            path: Path,
            source_url: str,
            licence: str,
            attribution: str = "",
            notes: str = "",
        ) -> str:
            calls["add"] = (path, source_url, licence, attribution, notes)
            return "clip-123"

        def write_manifest(self) -> str:
            calls["write_manifest"] = True
            return "deadbeef" * 8

    monkeypatch.setattr("ytauto.cli.__main__.BrollLibrary", _FakeLibrary)

    exit_code = main(
        [
            "--data-dir",
            str(tmp_path),
            "broll",
            "add",
            str(source_file),
            "--source-url",
            "https://example.com/clip",
            "--licence",
            "CC0",
            "--attribution",
            "Jane",
            "--notes",
            "stock",
        ]
    )

    assert exit_code == 0
    assert calls["add"] == (source_file, "https://example.com/clip", "CC0", "Jane", "stock")
    assert calls["write_manifest"] is True


def test_write_manifest_is_not_called_when_add_raises(
    tmp_path: Path, source_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed ingest must not rewrite the manifest - there is nothing new to
    describe, and doing so would be a pointless CAS write at best."""
    calls: dict[str, object] = {}

    class _FakeLibrary:
        def __init__(self, conn: object, cas: object) -> None:
            pass

        def add(self, *args: object, **kwargs: object) -> str:
            raise RuntimeError("boom")

        def write_manifest(self) -> str:
            calls["write_manifest"] = True
            return "deadbeef" * 8

    monkeypatch.setattr("ytauto.cli.__main__.BrollLibrary", _FakeLibrary)

    with pytest.raises(RuntimeError, match="boom"):
        main(
            [
                "--data-dir",
                str(tmp_path),
                "broll",
                "add",
                str(source_file),
                "--source-url",
                "local",
                "--licence",
                "CC0",
            ]
        )

    assert "write_manifest" not in calls
