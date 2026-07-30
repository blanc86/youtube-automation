import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_import_linter_contracts_hold() -> None:
    """The layering rules are executable, not aspirational."""
    exe_name = "lint-imports.exe" if sys.platform == "win32" else "lint-imports"
    exe = Path(sys.executable).parent / exe_name
    assert exe.is_file(), "dev extras missing; run: pip install -e '.[dev]'"

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
