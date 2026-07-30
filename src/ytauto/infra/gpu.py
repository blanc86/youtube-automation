"""NVIDIA GPU detection.

Absence of a GPU is a supported configuration, not an error: the render
pipeline falls back to QSV or libx264. Phase 1's resource governor sizes the
``gpu_compute`` lease pool from ``vram_mb``.
"""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass

_QUERY = "--query-gpu=name,memory.total,driver_version"
_FORMAT = "--format=csv,noheader"


@dataclass(frozen=True)
class GpuInfo:
    name: str
    vram_mb: int
    driver: str


def parse_nvidia_smi(csv_output: str) -> GpuInfo | None:
    """Parse ``nvidia-smi`` CSV output. Returns None if unusable. Pure."""
    for line in csv_output.strip().splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) != 3:
            continue
        name, memory, driver = parts
        digits = memory.split()[0] if memory else ""
        if not digits.isdigit():
            continue
        return GpuInfo(name=name, vram_mb=int(digits), driver=driver)
    return None


def detect() -> GpuInfo | None:
    """Detect the first NVIDIA GPU, or None when nvidia-smi is absent or fails.

    Raises nothing. Absence of a GPU is a supported configuration, not an
    error, and a machine that cannot be interrogated is indistinguishable from
    one with no GPU as far as encoder selection is concerned - so every
    subprocess failure is folded into None rather than propagated.
    """
    executable = shutil.which("nvidia-smi")
    if executable is None:
        return None
    try:
        result = subprocess.run(
            [executable, _QUERY, _FORMAT],
            capture_output=True,
            # Explicit over text=True: text=True decodes with the locale
            # encoding, so a GPU name containing a non-ASCII byte raises
            # UnicodeDecodeError on a cp1252 console. A mangled character in a
            # display string must never be able to fail detection.
            encoding="utf-8",
            errors="replace",
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return parse_nvidia_smi(result.stdout)
