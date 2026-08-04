"""The quality gate. One script, every platform, one source of truth.

Run it directly (``python scripts/check.py``) or through the thin
``check.ps1`` / ``check.sh`` wrappers, which exist only for muscle memory.

Two hard-won rules are encoded here rather than left to a shell:

1. **Every step's exit code is checked explicitly.** The PowerShell version of
   this gate reported ``ALL CHECKS PASSED`` for eight consecutive tasks while
   running only import-linter, because ``$ErrorActionPreference`` does not apply
   to native executable exit codes in Windows PowerShell 5.1. ``subprocess``
   returncodes cannot silently lie the same way.
2. **import-linter must be the console script.** ``python -m importlinter.cli``
   is not a substitute: that module has no ``__main__`` guard, so it imports,
   does nothing, and exits 0 - a second silently-passing gate, found only
   because someone tried to make it fail.

If you add a step here, demonstrate it failing before you trust it.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ANSI is fine on modern Windows terminals, macOS and Linux; disabled when the
# output is redirected so logs stay clean.
_TTY = sys.stdout.isatty()
CYAN = "\033[36m" if _TTY else ""
RED = "\033[31m" if _TTY else ""
GREEN = "\033[32m" if _TTY else ""
RESET = "\033[0m" if _TTY else ""


def _venv_bin(name: str) -> Path | None:
    """Locate an executable inside the project venv, if there is one.

    Windows puts console scripts in ``Scripts/`` with an ``.exe`` suffix;
    everything else uses ``bin/`` with no suffix.
    """
    if sys.platform == "win32":
        candidate = ROOT / ".venv" / "Scripts" / f"{name}.exe"
    else:
        candidate = ROOT / ".venv" / "bin" / name
    return candidate if candidate.is_file() else None


def python_executable() -> str:
    """The interpreter to run the gate's tools with.

    Prefers the project venv, falls back to the running interpreter. CI installs
    into the runner's own interpreter and has no ``.venv``, and this script is
    CI's single gate step - hardcoding a venv path would break it there.
    """
    venv_python = _venv_bin("python")
    return str(venv_python) if venv_python else sys.executable


def lint_imports_executable() -> str:
    """The import-linter console script. See rule 2 in the module docstring."""
    venv_script = _venv_bin("lint-imports")
    return str(venv_script) if venv_script else "lint-imports"


def run(label: str, argv: list[str]) -> None:
    """Run one gate step, aborting the whole gate if it fails.

    Raises:
        SystemExit: with code 1 if the step exits non-zero, or if its executable
            cannot be found at all - a missing tool is a failed gate, never a
            skipped one.
    """
    print(f"{CYAN}== {label} =={RESET}", flush=True)
    try:
        completed = subprocess.run(argv, cwd=ROOT, check=False)
    except FileNotFoundError:
        print(f"{RED}{label} FAILED - {argv[0]} not found{RESET}", flush=True)
        raise SystemExit(1) from None
    if completed.returncode != 0:
        print(f"{RED}{label} FAILED{RESET}", flush=True)
        raise SystemExit(1)


def main() -> int:
    """Run every gate step in order. Returns 0 only if all of them pass."""
    py = python_executable()

    run("ruff check", [py, "-m", "ruff", "check", "src", "tests"])
    run("ruff format", [py, "-m", "ruff", "format", "--check", "src", "tests"])
    run("mypy", [py, "-m", "mypy"])
    run("import-linter", [lint_imports_executable()])

    # addopts carries -m "not integration", so this is the hermetic suite only.
    run("pytest (unit)", [py, "-m", "pytest"])

    # Separate step because these shell out to a real ffmpeg and nvidia-smi.
    # Keeping them out of the default run stops the hermetic suite depending on
    # the host, while this step keeps them genuinely gated rather than quietly
    # skipped.
    run("pytest (integration)", [py, "-m", "pytest", "-m", "integration"])

    print(f"{GREEN}ALL CHECKS PASSED{RESET}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
