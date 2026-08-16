"""Unit tests for ``infra.ffmpeg.media_probe``.

``subprocess.run`` is monkeypatched throughout - these tests pin the JSON
parsing and the duration fallback/rejection policy, not real ffprobe
behaviour. ``tests/integration/test_broll_ingest.py`` covers the real binary.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from ytauto.core.errors import ValidationError
from ytauto.infra.ffmpeg.media_probe import MediaInfo, probe_dimensions, probe_media

_FFPROBE = Path("ffprobe.exe")


def _fake_run(stdout: str = "", returncode: int = 0, stderr: str = "") -> object:
    def _run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=[], returncode=returncode, stdout=stdout, stderr=stderr
        )

    return _run


def _payload(*, streams: list[dict[str, object]], fmt: dict[str, object]) -> str:
    return json.dumps({"streams": streams, "format": fmt})


def _video_stream(**overrides: object) -> dict[str, object]:
    stream: dict[str, object] = {"codec_type": "video", "width": 1920, "height": 1080}
    stream.update(overrides)
    return stream


@pytest.fixture()
def existing_file(tmp_path: Path) -> Path:
    src = tmp_path / "clip.mp4"
    src.write_bytes(b"not real video, ffprobe is mocked")
    return src


def test_probes_width_height_and_duration_from_the_format_block(
    monkeypatch: pytest.MonkeyPatch, existing_file: Path
) -> None:
    payload = _payload(streams=[_video_stream()], fmt={"duration": "12.500000"})
    monkeypatch.setattr("ytauto.infra.ffmpeg.media_probe.subprocess.run", _fake_run(stdout=payload))

    info = probe_media(existing_file, ffprobe=_FFPROBE)

    assert info == MediaInfo(width=1920, height=1080, duration_s=12.5)


def test_falls_back_to_the_video_stream_duration_when_format_omits_it(
    monkeypatch: pytest.MonkeyPatch, existing_file: Path
) -> None:
    """Some inputs carry a duration on the stream but not the format block."""
    payload = _payload(streams=[_video_stream(duration="3.000000")], fmt={})
    monkeypatch.setattr("ytauto.infra.ffmpeg.media_probe.subprocess.run", _fake_run(stdout=payload))

    info = probe_media(existing_file, ffprobe=_FFPROBE)

    assert info.duration_s == 3.0


def test_a_zero_format_duration_falls_through_to_the_stream_duration(
    monkeypatch: pytest.MonkeyPatch, existing_file: Path
) -> None:
    """A zero duration must never be accepted as-is - a black gap otherwise."""
    payload = _payload(streams=[_video_stream(duration="4.000000")], fmt={"duration": "0.000000"})
    monkeypatch.setattr("ytauto.infra.ffmpeg.media_probe.subprocess.run", _fake_run(stdout=payload))

    info = probe_media(existing_file, ffprobe=_FFPROBE)

    assert info.duration_s == 4.0


def test_no_positive_duration_anywhere_is_a_validation_error(
    monkeypatch: pytest.MonkeyPatch, existing_file: Path
) -> None:
    """Mutation guard: both duration fields absent/zero must fail loudly, not
    default to zero. A zero-duration clip would still be selectable for a
    segment and render as a silent black gap."""
    payload = _payload(streams=[_video_stream(duration="0.0")], fmt={"duration": "0.0"})
    monkeypatch.setattr("ytauto.infra.ffmpeg.media_probe.subprocess.run", _fake_run(stdout=payload))

    with pytest.raises(ValidationError, match="duration"):
        probe_media(existing_file, ffprobe=_FFPROBE)


def test_missing_video_stream_is_a_validation_error(
    monkeypatch: pytest.MonkeyPatch, existing_file: Path
) -> None:
    payload = _payload(streams=[{"codec_type": "audio"}], fmt={"duration": "1.0"})
    monkeypatch.setattr("ytauto.infra.ffmpeg.media_probe.subprocess.run", _fake_run(stdout=payload))

    with pytest.raises(ValidationError, match="no video stream"):
        probe_media(existing_file, ffprobe=_FFPROBE)


def test_video_stream_missing_dimensions_is_a_validation_error(
    monkeypatch: pytest.MonkeyPatch, existing_file: Path
) -> None:
    payload = _payload(streams=[{"codec_type": "video", "width": 1920}], fmt={"duration": "1.0"})
    monkeypatch.setattr("ytauto.infra.ffmpeg.media_probe.subprocess.run", _fake_run(stdout=payload))

    with pytest.raises(ValidationError, match="width/height"):
        probe_media(existing_file, ffprobe=_FFPROBE)


def test_a_nonzero_ffprobe_exit_is_a_validation_error(
    monkeypatch: pytest.MonkeyPatch, existing_file: Path
) -> None:
    monkeypatch.setattr(
        "ytauto.infra.ffmpeg.media_probe.subprocess.run",
        _fake_run(returncode=1, stderr="moov atom not found"),
    )

    with pytest.raises(ValidationError, match="moov atom not found"):
        probe_media(existing_file, ffprobe=_FFPROBE)


def test_unparseable_json_is_a_validation_error(
    monkeypatch: pytest.MonkeyPatch, existing_file: Path
) -> None:
    monkeypatch.setattr(
        "ytauto.infra.ffmpeg.media_probe.subprocess.run", _fake_run(stdout="not json")
    )

    with pytest.raises(ValidationError, match="unparseable"):
        probe_media(existing_file, ffprobe=_FFPROBE)


def test_a_missing_source_file_is_a_validation_error_before_any_subprocess_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def _boom(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("subprocess.run must not be called for a missing source file")

    monkeypatch.setattr("ytauto.infra.ffmpeg.media_probe.subprocess.run", _boom)

    with pytest.raises(ValidationError, match="does not exist"):
        probe_media(tmp_path / "missing.mp4", ffprobe=_FFPROBE)


def test_probe_dimensions_returns_only_the_width_height_pair(
    monkeypatch: pytest.MonkeyPatch, existing_file: Path
) -> None:
    payload = _payload(streams=[_video_stream(width=640, height=480)], fmt={"duration": "2.0"})
    monkeypatch.setattr("ytauto.infra.ffmpeg.media_probe.subprocess.run", _fake_run(stdout=payload))

    assert probe_dimensions(existing_file, ffprobe=_FFPROBE) == (640, 480)
