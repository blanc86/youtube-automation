"""Discover what the located ffmpeg build can actually do.

Both parsers are pure functions over captured text, so encoder-selection
policy is fully tested without invoking a binary.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass

from ytauto.core.errors import ConfigurationError
from ytauto.infra.ffmpeg.locator import FfmpegBinaries

ENCODER_PREFERENCE: tuple[str, ...] = ("h264_nvenc", "h264_qsv", "libx264")

# ffmpeg lists capabilities as: flag characters, whitespace, then the name.
# Legend lines (e.g. " V..... = Video") have the same flag-then-token shape,
# but the token is a literal "=" — never a real capability name — so that
# token is excluded rather than trusted.
_ENCODER_RE = re.compile(r"^\s*[A-Z.]{6}\s+(\S+)")
_FILTER_RE = re.compile(r"^\s*[A-Z.]{3}\s+(\S+)")


def parse_encoders(output: str) -> frozenset[str]:
    """Extract encoder names from ``ffmpeg -encoders`` output. Pure."""
    return frozenset(
        match.group(1)
        for line in output.splitlines()
        if (match := _ENCODER_RE.match(line)) and match.group(1) != "="
    )


def parse_filters(output: str) -> frozenset[str]:
    """Extract filter names from ``ffmpeg -filters`` output. Pure."""
    return frozenset(
        match.group(1)
        for line in output.splitlines()
        if (match := _FILTER_RE.match(line)) and match.group(1) != "="
    )


@dataclass(frozen=True)
class FfmpegCapabilities:
    encoders: frozenset[str]
    filters: frozenset[str]

    def best_h264_encoder(self) -> str:
        """Pick the fastest available H.264 encoder: NVENC, then QSV, then libx264."""
        for candidate in ENCODER_PREFERENCE:
            if candidate in self.encoders:
                return candidate
        raise ConfigurationError(
            "this ffmpeg build has no usable h264 encoder "
            f"(looked for {', '.join(ENCODER_PREFERENCE)})"
        )

    def has_subtitle_burn_in(self) -> bool:
        """Caption rendering needs libass; without it no video can be subtitled."""
        return "ass" in self.filters


def _run(binary: str, flag: str) -> str:
    result = subprocess.run(  # noqa: S603
        [binary, "-hide_banner", flag],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    return result.stdout


def probe(binaries: FfmpegBinaries) -> FfmpegCapabilities:
    """Query the binary once for its encoders and filters."""
    return FfmpegCapabilities(
        encoders=parse_encoders(_run(str(binaries.ffmpeg), "-encoders")),
        filters=parse_filters(_run(str(binaries.ffmpeg), "-filters")),
    )
