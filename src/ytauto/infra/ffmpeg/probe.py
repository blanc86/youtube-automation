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
_ENCODER_RE = re.compile(r"^\s*[A-Z.]{6,}\s+(\S+)")
_FILTER_RE = re.compile(r"^\s*[A-Z.]{3,}\s+(\S+)\s+\S*->\S*")
"""Deliberately not a fixed-width flag column.

``ffmpeg -filters`` prints ``FLAGS NAME INPUTS->OUTPUTS DESCRIPTION``. Pinning
the flag column to exactly three characters made this parser silently
version-specific: on ffmpeg 9 it matched nothing at all, every capability came
back empty, and ``has_subtitle_burn_in`` then told the operator their build
"has no 'ass' filter (libass)" - about a gyan full build that ships libass.
CI hit exactly that, and a message that confidently names the wrong cause is
worse than no message.

So the flag column is ``{3,}``/``{6,}`` (a future flag cannot break it) and
filters additionally anchor on the ``->`` in the signature column, which has
been stable for many releases and is what makes a widened flag column
unambiguous rather than a guess about where the name starts. That anchor also
does the header/legend exclusion structurally: ``Filters:`` and the ``=``
legend row carry no signature."""


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
        """Pick the fastest available H.264 encoder: NVENC, then QSV, then libx264.

        Raises:
            ConfigurationError: the build exposes none of them, so it cannot
                produce H.264 at all.
        """
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
    """Ask the binary one capability question and return its stdout.

    Raises rather than returning ``""`` on a non-zero exit. An empty string
    parses to an empty capability set, which every caller then reports as a
    *missing feature* - "this build has no h264 encoder", "this build has no
    'ass' filter" - when the truth is that the probe never ran. That failure
    mode blames the user's ffmpeg for something this code did, and it is the
    hardest kind of message to debug because it is specific and wrong.

    Raises:
        ConfigurationError: ffmpeg exited non-zero.
    """
    result = subprocess.run(
        [binary, "-hide_banner", flag],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        raise ConfigurationError(
            f"{binary} {flag} exited {result.returncode}, so this build's capabilities "
            f"could not be determined: {result.stderr.strip()[:400]}"
        )
    return result.stdout


def probe(binaries: FfmpegBinaries) -> FfmpegCapabilities:
    """Query the binary once for its encoders and filters.

    Raises:
        subprocess.TimeoutExpired: a probe call did not respond within 30s.
            This is a SubprocessError, NOT an OSError - catching only OSError
            here lets a hung ffmpeg escape.
        OSError: the binary cannot be executed.
    """
    return FfmpegCapabilities(
        encoders=parse_encoders(_run(str(binaries.ffmpeg), "-encoders")),
        filters=parse_filters(_run(str(binaries.ffmpeg), "-filters")),
    )
