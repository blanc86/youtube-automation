"""Environment checks. The Phase 0 exit criterion.

Severity is meaningful: WARN means degraded but usable (no NVIDIA GPU - fall
back to libx264), FAIL means a required capability is absent (no libass means
no video can ever be subtitled).
"""

from __future__ import annotations

import contextlib
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from enum import StrEnum

from ytauto.core.errors import ConfigurationError
from ytauto.infra import gpu
from ytauto.infra.cas.eviction import EvictionPolicy
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import HEAD_VERSION, apply_migrations
from ytauto.infra.ffmpeg.locator import FfmpegNotFound, locate
from ytauto.infra.ffmpeg.probe import probe
from ytauto.infra.paths import AppPaths

_MIN_PYTHON = (3, 12)
_LOW_DISK_WARN_GB = 20


class Severity(StrEnum):
    OK = "ok"
    WARN = "warn"
    FAIL = "fail"


@dataclass(frozen=True)
class CheckResult:
    name: str
    severity: Severity
    detail: str


def _check_python() -> CheckResult:
    version = ".".join(str(part) for part in sys.version_info[:3])
    if sys.version_info >= _MIN_PYTHON:
        return CheckResult("python", Severity.OK, version)
    return CheckResult("python", Severity.FAIL, f"{version} (need >= 3.12)")


def _check_paths(paths: AppPaths) -> CheckResult:
    try:
        paths.ensure()
        probe_file = paths.cache / ".write-test"
        probe_file.write_text("ok", encoding="utf-8")
        probe_file.unlink()
    except (ConfigurationError, OSError) as exc:
        # ensure() raises ConfigurationError; the write probe raises raw OSError.
        return CheckResult("data directories", Severity.FAIL, f"{paths.root}: {exc}")
    return CheckResult("data directories", Severity.OK, str(paths.root))


def _check_database(paths: AppPaths) -> list[CheckResult]:
    results: list[CheckResult] = []
    try:
        # closing(): the connection must be released even when apply_migrations
        # or the ceiling calculation raises. With conn.close() as the last
        # statement of the try block, every failure below leaked it.
        with contextlib.closing(connect(paths.db_file)) as conn:
            version = apply_migrations(conn)
            results.append(
                CheckResult(
                    "database",
                    Severity.OK if version == HEAD_VERSION else Severity.FAIL,
                    f"schema v{version} (head v{HEAD_VERSION})",
                )
            )
            store = CasStore(root=paths.cas, conn=conn)
            current = store.total_size()
            policy = EvictionPolicy.compute(paths.cas, current_size=current)
            results.append(
                CheckResult(
                    "cache ceiling",
                    Severity.OK,
                    f"{current / 1024**3:.2f} GB used of {policy.max_bytes / 1024**3:.1f} GB",
                )
            )
    except (OSError, sqlite3.Error) as exc:
        # Fill in only the rows that were not reached. The ceiling calculation
        # runs after the database row is appended, so appending both
        # unconditionally would emit a second, contradictory "database" row.
        reported = {r.name for r in results}
        if "database" not in reported:
            results.append(CheckResult("database", Severity.FAIL, str(exc)))
        if "cache ceiling" not in reported:
            results.append(CheckResult("cache ceiling", Severity.FAIL, f"unavailable: {exc}"))
    return results


def _check_ffmpeg() -> list[CheckResult]:
    try:
        binaries = locate()
    except (FfmpegNotFound, OSError, subprocess.SubprocessError) as exc:
        # FfmpegNotFound covers plain absence. The other two cover a binary that
        # is present but unusable: locate() calls _read_version(), which runs a
        # subprocess with timeout=15, so a hanging ffmpeg raises TimeoutExpired
        # (a SubprocessError, NOT an OSError) and a non-executable one raises
        # OSError. Both are diagnoses doctor must print, not crash on.
        # str() not repr(): TimeoutExpired never populates .args, so its repr is
        # the empty "TimeoutExpired()". Its __str__ names the command and timeout.
        detail = (
            str(exc)
            if isinstance(exc, FfmpegNotFound)
            else f"ffmpeg is present but unusable: {type(exc).__name__}: {exc}"
        )
        return [
            CheckResult("ffmpeg", Severity.FAIL, detail),
            CheckResult("h264 encoder", Severity.FAIL, "ffmpeg unavailable"),
            CheckResult("subtitle burn-in", Severity.FAIL, "ffmpeg unavailable"),
        ]

    results = [CheckResult("ffmpeg", Severity.OK, f"{binaries.version} at {binaries.ffmpeg}")]
    try:
        capabilities = probe(binaries)
    except (OSError, subprocess.SubprocessError) as exc:
        # A present-but-broken binary (corrupt, unexecutable, hanging) is exactly
        # what doctor exists to surface. probe() can raise TimeoutExpired, which
        # is a SubprocessError and NOT an OSError. Letting it escape would show a
        # traceback instead of the diagnosis.
        detail = f"could not probe ffmpeg: {exc}"
        results.append(CheckResult("h264 encoder", Severity.FAIL, detail))
        results.append(CheckResult("subtitle burn-in", Severity.FAIL, detail))
        return results

    try:
        encoder = capabilities.best_h264_encoder()
        severity = Severity.OK if encoder != "libx264" else Severity.WARN
        detail = encoder if severity is Severity.OK else f"{encoder} (software only, slower)"
        results.append(CheckResult("h264 encoder", severity, detail))
    except ConfigurationError as exc:
        results.append(CheckResult("h264 encoder", Severity.FAIL, str(exc)))

    if capabilities.has_subtitle_burn_in():
        results.append(CheckResult("subtitle burn-in", Severity.OK, "libass available"))
    else:
        results.append(
            CheckResult(
                "subtitle burn-in",
                Severity.FAIL,
                "this ffmpeg build lacks the 'ass' filter; captions cannot be rendered",
            )
        )
    return results


def _check_gpu() -> CheckResult:
    info = gpu.detect()
    if info is None:
        return CheckResult("gpu", Severity.WARN, "no NVIDIA GPU detected; CPU encoding only")
    return CheckResult("gpu", Severity.OK, f"{info.name}, {info.vram_mb} MiB, driver {info.driver}")


def _check_disk(paths: AppPaths) -> CheckResult:
    try:
        paths.ensure()
        free_gb = shutil.disk_usage(paths.root).free / 1024**3
    except (ConfigurationError, OSError) as exc:
        # Never let a failed check abort the run - doctor must report every row.
        return CheckResult("free disk", Severity.FAIL, str(exc))
    severity = Severity.OK if free_gb >= _LOW_DISK_WARN_GB else Severity.WARN
    return CheckResult("free disk", severity, f"{free_gb:.1f} GB free")


def run_checks(paths: AppPaths) -> list[CheckResult]:
    """Run every environment check. Order is the order reported."""
    results = [_check_python(), _check_paths(paths)]
    results.extend(_check_database(paths))
    results.extend(_check_ffmpeg())
    results.append(_check_gpu())
    results.append(_check_disk(paths))
    return results


_GLYPH = {Severity.OK: "[ OK ]", Severity.WARN: "[WARN]", Severity.FAIL: "[FAIL]"}


def format_report(results: list[CheckResult]) -> str:
    width = max((len(r.name) for r in results), default=0)
    lines = [f"{_GLYPH[r.severity]}  {r.name.ljust(width)}  {r.detail}" for r in results]
    failures = sum(1 for r in results if r.severity is Severity.FAIL)
    warnings = sum(1 for r in results if r.severity is Severity.WARN)
    lines.append("")
    lines.append(
        "environment is GREEN" if failures == 0 else f"environment has {failures} failure(s)"
    )
    if warnings:
        lines.append(f"{warnings} warning(s)")
    return "\n".join(lines)


def exit_code(results: list[CheckResult]) -> int:
    return 1 if any(r.severity is Severity.FAIL for r in results) else 0
