import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _find_lint_imports() -> Path:
    """Locate the console script.

    In a venv it sits beside python.exe; in a plain CPython install (what CI
    uses) it lands in a Scripts/ subdirectory instead. PATH is the last resort.
    """
    exe_name = "lint-imports.exe" if sys.platform == "win32" else "lint-imports"
    base = Path(sys.executable).parent
    for candidate in (base / exe_name, base / "Scripts" / exe_name, base / "bin" / exe_name):
        if candidate.is_file():
            return candidate
    from shutil import which

    found = which("lint-imports")
    assert found is not None, "dev extras missing; run: pip install -e '.[dev]'"
    return Path(found)


def test_import_linter_contracts_hold() -> None:
    """The layering rules are executable, not aspirational."""
    exe = _find_lint_imports()

    env = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
    result = subprocess.run(
        [str(exe)],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        cwd=REPO_ROOT,
        env=env,
    )
    assert result.returncode == 0, result.stdout + result.stderr
