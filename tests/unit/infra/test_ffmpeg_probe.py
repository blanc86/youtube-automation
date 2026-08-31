import pytest

from ytauto.core.errors import ConfigurationError
from ytauto.infra.ffmpeg.probe import (
    FfmpegCapabilities,
    parse_encoders,
    parse_filters,
)

# Captured verbatim from `ffmpeg -hide_banner -encoders` on the target machine.
ENCODERS_OUTPUT = """Encoders:
 V..... = Video
 ------
 V....D av1_nvenc            NVIDIA NVENC av1 encoder (codec av1)
 V..... av1_qsv              AV1 (Intel Quick Sync Video acceleration) (codec av1)
 V....D libx264              libx264 H.264 / AVC / MPEG-4 AVC (codec h264)
 V....D h264_nvenc           NVIDIA NVENC H.264 encoder (codec h264)
 V..... h264_qsv             H.264 / AVC (Intel Quick Sync Video acceleration) (codec h264)
 V....D libx265              libx265 H.265 / HEVC (codec hevc)
 A....D aac                  AAC (Advanced Audio Coding)
"""

FILTERS_OUTPUT = """Filters:
  T.. = Timeline support
  ... ass               V->V       Render ASS subtitles onto input video using the libass library.
  ... subtitles         V->V       Render text subtitles onto input video using the libass library.
  ... overlay_cuda      VV->V      Overlay one video on top of another using CUDA
  ... scale_cuda        V->V       GPU accelerated video resizer
  ... zoompan           V->V       Apply Zoom & Pan effect.
  .S. xfade             VV->V      Cross fade one video with another video.
"""


def test_parse_encoders_finds_hardware_and_software_encoders() -> None:
    encoders = parse_encoders(ENCODERS_OUTPUT)
    assert "h264_nvenc" in encoders
    assert "h264_qsv" in encoders
    assert "libx264" in encoders
    assert "aac" in encoders


def test_parse_encoders_excludes_header_and_legend_lines() -> None:
    encoders = parse_encoders(ENCODERS_OUTPUT)
    assert "Encoders:" not in encoders
    assert "Video" not in encoders
    assert "=" not in encoders


def test_parse_filters_finds_subtitle_and_cuda_filters() -> None:
    filters = parse_filters(FILTERS_OUTPUT)
    assert {"ass", "subtitles", "scale_cuda", "zoompan", "xfade"} <= filters


def test_parse_filters_excludes_header_and_legend_lines() -> None:
    """Mirrors the encoders exclusion test.

    The fixture previously had no legend row, so deleting the
    `and match.group(1) != "="` guard from parse_filters broke no test.
    """
    filters = parse_filters(FILTERS_OUTPUT)
    assert "Filters:" not in filters
    assert "=" not in filters
    assert "Timeline" not in filters


def test_nvenc_is_preferred_when_available() -> None:
    caps = FfmpegCapabilities(
        encoders=parse_encoders(ENCODERS_OUTPUT), filters=parse_filters(FILTERS_OUTPUT)
    )
    assert caps.best_h264_encoder() == "h264_nvenc"


def test_falls_back_to_qsv_without_nvenc() -> None:
    caps = FfmpegCapabilities(encoders=frozenset({"h264_qsv", "libx264"}), filters=frozenset())
    assert caps.best_h264_encoder() == "h264_qsv"


def test_falls_back_to_libx264_as_last_resort() -> None:
    caps = FfmpegCapabilities(encoders=frozenset({"libx264"}), filters=frozenset())
    assert caps.best_h264_encoder() == "libx264"


def test_no_h264_encoder_at_all_is_a_configuration_error() -> None:
    caps = FfmpegCapabilities(encoders=frozenset({"libx265"}), filters=frozenset())
    with pytest.raises(ConfigurationError, match="h264"):
        caps.best_h264_encoder()


def test_subtitle_burn_in_requires_the_ass_filter() -> None:
    assert FfmpegCapabilities(
        encoders=frozenset(), filters=frozenset({"ass", "subtitles"})
    ).has_subtitle_burn_in()
    assert not FfmpegCapabilities(
        encoders=frozenset(), filters=frozenset({"zoompan"})
    ).has_subtitle_burn_in()


def test_the_subtitles_filter_alone_is_not_enough() -> None:
    """Pins that the requirement is the `ass` filter specifically.

    A build can expose `subtitles` without `ass`; the styled captions this
    pipeline renders need libass, so that build is unusable.
    """
    assert not FfmpegCapabilities(
        encoders=frozenset(), filters=frozenset({"subtitles"})
    ).has_subtitle_burn_in()


# -- version robustness: the flag column is not a fixed width -------------------

FILTERS_OUTPUT_FFMPEG_9 = """Filters:
  T.. = Timeline support
  .S. = Slice threading
  A->A = Audio input/output
 .. ass               V->V       Render ASS subtitles onto input video using the libass library.
 TS volume            A->A       Change input volume.
 .. amix              N->A       Audio mixing.
"""


def test_filters_parse_on_ffmpeg_9_which_prints_two_flag_characters() -> None:
    """Captured verbatim from a CI run on ffmpeg 9.0.1, which prints two flag
    characters where 7.1.1 printed three.

    A fixed-width flag column matched nothing on that build, every capability
    came back empty, and the operator was told a gyan full build compiled with
    --enable-libass "has no 'ass' filter (libass)".
    """
    filters = parse_filters(FILTERS_OUTPUT_FFMPEG_9)
    assert "ass" in filters
    assert {"volume", "amix"} <= filters


def test_the_legend_never_becomes_a_filter_name() -> None:
    """Structural, not a special case: legend and header rows carry no
    ``inputs->outputs`` signature, so the anchor excludes them."""
    filters = parse_filters(FILTERS_OUTPUT_FFMPEG_9)
    assert "Filters:" not in filters
    assert "=" not in filters
    assert "Timeline" not in filters
    # "A->A = Audio input/output" is a legend row that DOES contain "->";
    # it must still not be read as a filter named "=".
    assert not any(name.startswith("=") for name in filters)


def test_a_probe_that_cannot_run_is_not_reported_as_a_missing_feature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Returning "" on a failed probe made every capability look absent, so the
    error named a feature the build actually had."""
    import subprocess as _subprocess

    from ytauto.infra.ffmpeg import probe as probe_module

    class _Failed:
        returncode = 1
        stdout = ""
        stderr = "ffmpeg: cannot open shared library"

    monkeypatch.setattr(_subprocess, "run", lambda *a, **k: _Failed())
    with pytest.raises(ConfigurationError, match="capabilities could not be determined"):
        probe_module._run("ffmpeg", "-filters")
