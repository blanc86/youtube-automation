# Phase 0: Foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the runnable substrate every later phase depends on — toolchain, paths, logging, error taxonomy, SQLite with migrations, the content-addressed store with eviction, FFmpeg discovery/probing, and a `ytauto doctor` command that proves the environment is green.

**Architecture:** Strict inward-only layering enforced by `import-linter` in CI from the very first commit. `core/` is pure Python with no I/O; `infra/` owns every side effect. Anything that parses text (FFmpeg output, `nvidia-smi` output, log records) is split into a **pure parser function** plus a thin subprocess caller, so the logic is testable without executing binaries or touching a network.

**Tech Stack:** Python 3.12, stdlib `sqlite3` (WAL), `platformdirs`, `argparse`, pytest, mypy, ruff, import-linter. FFmpeg 7.1.1 is an external binary, never a Python dependency.

## Global Constraints

Every task's requirements implicitly include this section.

- **Python 3.12** is the target interpreter. The machine currently has only 3.10.0; Task 1 installs 3.12.
- **`src/` layout.** All package code lives under `src/ytauto/`. Tests run against the installed package.
- **`core/` imports nothing but the standard library.** No `infra`, no `app`, no `providers`, no `ui`, no `PySide6`, no network libraries.
- **No layer below `ui/` may import `PySide6`.** Phase 0 adds no Qt at all.
- **Every text-parsing routine is a pure function** taking `str` and returning a value. Subprocess invocation lives in a separate function. This is non-negotiable: it is what keeps the test suite fast and network/binary-free.
- **No bare `except`.** Every caught exception is a specific type and is either handled or re-raised as a typed error from `ytauto.core.errors`.
- **Content hashes are SHA-256, lowercase hex, full 64 chars.** Never truncated.
- **All timestamps are UTC, stored as ISO-8601 strings** with explicit `+00:00` offset.
- **Commit after every task.** Conventional-commit prefixes (`feat:`, `test:`, `chore:`, `fix:`, `docs:`).
- Spec of record: `docs/superpowers/specs/2026-07-30-youtube-automation-design.md`

---

## File Structure

Files created in this phase, and the single responsibility of each:

| File | Responsibility |
|---|---|
| `pyproject.toml` | Dependencies, and the config for ruff / mypy / pytest / import-linter |
| `src/ytauto/__init__.py` | Package version constant |
| `src/ytauto/core/errors.py` | The typed error taxonomy. Pure. |
| `src/ytauto/infra/paths.py` | Resolve and create every directory the app writes to |
| `src/ytauto/infra/clock.py` | The single source of UTC ISO-8601 timestamps |
| `src/ytauto/infra/logging.py` | JSON-lines structured logging + correlation-ID propagation |
| `src/ytauto/infra/db/engine.py` | SQLite connection with the correct pragmas, plus a transaction helper |
| `src/ytauto/infra/db/migrations.py` | Ordered, idempotent schema migrations |
| `src/ytauto/infra/cas/store.py` | Content-addressed blob storage with refcounts |
| `src/ytauto/infra/cas/eviction.py` | LRU eviction against a computed size ceiling |
| `src/ytauto/infra/ffmpeg/locator.py` | Find the `ffmpeg`/`ffprobe` binaries and read their version |
| `src/ytauto/infra/ffmpeg/probe.py` | Parse available encoders/filters; select the best H.264 encoder |
| `src/ytauto/infra/gpu.py` | Detect NVIDIA GPU name / VRAM / driver |
| `src/ytauto/cli/doctor.py` | Environment checks returning structured results |
| `src/ytauto/cli/__main__.py` | `argparse` entry point dispatching subcommands |
| `scripts/check.ps1` | Run the whole quality gate locally |
| `.github/workflows/ci.yml` | Same gate in CI, active the moment a remote exists |

**Decomposition note:** `cas/store.py` and `cas/eviction.py` are split because they have genuinely different reasons to change — storage layout versus retention policy. `ffmpeg/locator.py` and `ffmpeg/probe.py` split for the same reason: *where is the binary* versus *what can it do*.

---

## Task 1: Toolchain, package skeleton, and the architecture gate

**Files:**
- Create: `pyproject.toml`
- Create: `src/ytauto/__init__.py`
- Create: `src/ytauto/{core,app,providers,infra,ui,cli}/__init__.py` and subpackage inits
- Create: `scripts/check.ps1`
- Create: `.github/workflows/ci.yml`
- Test: `tests/unit/test_package.py`, `tests/unit/test_architecture.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `ytauto.__version__: str`. An importable package skeleton with every subpackage present, so `import-linter` contracts can resolve module names from here on.

**Why the empty packages exist now:** `import-linter`'s `layers` contract errors if a named module is missing. Creating the full skeleton in Task 1 means the architecture gate is live from the first commit rather than being retrofitted once violations already exist.

- [ ] **Step 1: Install Python 3.12**

```powershell
winget install --id Python.Python.3.12 -e --source winget
```

Then open a **new** shell (winget edits PATH) and verify:

```powershell
py -3.12 --version
```

Expected: `Python 3.12.x`

- [ ] **Step 2: Create the virtual environment**

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
```

- [ ] **Step 3: Write `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "ytauto"
version = "0.1.0"
description = "Faceless YouTube video automation"
requires-python = ">=3.12"
dependencies = [
    "platformdirs>=4.2",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-cov>=5.0",
    "mypy>=1.10",
    "ruff>=0.5",
    "import-linter>=2.0",
]

[project.scripts]
ytauto = "ytauto.cli.__main__:main"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-q --strict-markers"

[tool.ruff]
line-length = 100
src = ["src", "tests"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "PTH"]

[tool.mypy]
python_version = "3.12"
packages = ["ytauto"]
mypy_path = "src"

[[tool.mypy.overrides]]
module = "ytauto.core.*"
strict = true

[tool.importlinter]
root_package = "ytauto"

[[tool.importlinter.contracts]]
name = "core is pure - depends on nothing internal"
type = "forbidden"
source_modules = ["ytauto.core"]
forbidden_modules = [
    "ytauto.infra",
    "ytauto.app",
    "ytauto.providers",
    "ytauto.ui",
    "ytauto.cli",
]

[[tool.importlinter.contracts]]
name = "engine never imports Qt"
type = "forbidden"
source_modules = [
    "ytauto.core",
    "ytauto.app",
    "ytauto.providers",
    "ytauto.infra",
    "ytauto.cli",
]
forbidden_modules = ["PySide6"]

[[tool.importlinter.contracts]]
name = "layered architecture"
type = "layers"
layers = ["ytauto.ui", "ytauto.app", "ytauto.core"]
```

- [ ] **Step 4: Create the package skeleton**

```powershell
$dirs = @(
  "src/ytauto/core/models","src/ytauto/core/ports","src/ytauto/core/pipeline/stages",
  "src/ytauto/core/policy","src/ytauto/app/services","src/ytauto/app/scheduler",
  "src/ytauto/app/worker","src/ytauto/providers","src/ytauto/infra/db",
  "src/ytauto/infra/cas","src/ytauto/infra/ffmpeg","src/ytauto/infra/secrets",
  "src/ytauto/ui","src/ytauto/cli",
  "tests/unit","tests/contract","tests/integration","tests/golden","scripts"
)
foreach ($d in $dirs) {
  New-Item -ItemType Directory -Force -Path $d | Out-Null
  if ($d -like "src/*") {
    $init = Join-Path $d "__init__.py"
    if (-not (Test-Path $init)) { New-Item -ItemType File -Path $init | Out-Null }
  }
}
```

Write `src/ytauto/__init__.py`:

```python
"""ytauto - faceless YouTube video automation."""

__version__ = "0.1.0"
```

- [ ] **Step 5: Write the failing tests**

`tests/unit/test_package.py`:

```python
import ytauto


def test_package_exposes_version() -> None:
    assert ytauto.__version__ == "0.1.0"


def test_running_on_python_312_or_newer() -> None:
    import sys

    assert sys.version_info >= (3, 12), f"expected >=3.12, got {sys.version_info}"
```

`tests/unit/test_architecture.py`:

```python
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_import_linter_contracts_hold() -> None:
    """The layering rules are executable, not aspirational."""
    exe_name = "lint-imports.exe" if sys.platform == "win32" else "lint-imports"
    exe = Path(sys.executable).parent / exe_name
    assert exe.is_file(), "dev extras missing; run: pip install -e '.[dev]'"

    result = subprocess.run(
        [str(exe)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    assert result.returncode == 0, result.stdout + result.stderr
```

`lint-imports` is the console script import-linter installs; there is no
`importlinter.__main__`, so invoking it via `-m` would fail. `cwd` must be the
repository root so the tool finds the contracts in `pyproject.toml`.

- [ ] **Step 6: Run the tests to verify they fail**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/test_package.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'ytauto'` (not yet installed).

- [ ] **Step 7: Install the package in editable mode**

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

- [ ] **Step 8: Run the tests to verify they pass**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit -v
```

Expected: 3 passed.

- [ ] **Step 9: Write the local quality gate**

`scripts/check.ps1`:

```powershell
$ErrorActionPreference = "Stop"
$py = ".\.venv\Scripts\python.exe"

Write-Host "== ruff ==" -ForegroundColor Cyan
& $py -m ruff check src tests
& $py -m ruff format --check src tests

Write-Host "== mypy ==" -ForegroundColor Cyan
& $py -m mypy

Write-Host "== import-linter ==" -ForegroundColor Cyan
& ".\.venv\Scripts\lint-imports.exe"
if ($LASTEXITCODE -ne 0) { throw "import-linter contracts violated" }

Write-Host "== pytest ==" -ForegroundColor Cyan
& $py -m pytest

Write-Host "ALL CHECKS PASSED" -ForegroundColor Green
```

`.github/workflows/ci.yml`:

```yaml
name: CI
on: [push, pull_request]

jobs:
  check:
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -e ".[dev]"
      - run: ruff check src tests
      - run: mypy
      - run: lint-imports
      - run: pytest
```

- [ ] **Step 10: Run the full gate**

```powershell
powershell -ExecutionPolicy Bypass -File scripts/check.ps1
```

Expected: `ALL CHECKS PASSED`. Fix any ruff formatting complaints with `.\.venv\Scripts\python.exe -m ruff format src tests`.

- [ ] **Step 11: Commit**

```bash
git add pyproject.toml src tests scripts .github
git commit -m "chore: scaffold package, toolchain, and executable architecture gate"
```

---

## Task 2: Typed error taxonomy

**Files:**
- Create: `src/ytauto/core/errors.py`
- Test: `tests/unit/core/test_errors.py`

**Interfaces:**
- Consumes: nothing (pure stdlib).
- Produces:
  - `YtautoError(Exception)` — base of every application error
  - `ValidationError`, `ResourceExhausted`, `RenderError`, `ConfigurationError` — all subclass `YtautoError`
  - `ErrorKind` — `StrEnum` with `RETRYABLE`, `FATAL`, `RATE_LIMITED`, `QUOTA_EXCEEDED`
  - `ProviderError(message: str, *, provider_id: str, kind: ErrorKind, retry_after_s: float | None = None)` with property `is_retryable: bool`

Every later task raises from this module. Nothing raises bare `Exception`.

- [ ] **Step 1: Write the failing test**

`tests/unit/core/test_errors.py` (create `tests/unit/core/__init__.py` as an empty file first):

```python
import pytest

from ytauto.core.errors import (
    ErrorKind,
    ProviderError,
    RenderError,
    ValidationError,
    YtautoError,
)


def test_all_errors_share_one_base() -> None:
    for cls in (ValidationError, RenderError, ProviderError):
        assert issubclass(cls, YtautoError)


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        (ErrorKind.RETRYABLE, True),
        (ErrorKind.RATE_LIMITED, True),
        (ErrorKind.FATAL, False),
        (ErrorKind.QUOTA_EXCEEDED, False),
    ],
)
def test_retryability_is_derived_from_kind(kind: ErrorKind, expected: bool) -> None:
    err = ProviderError("boom", provider_id="gemini", kind=kind)
    assert err.is_retryable is expected


def test_quota_exceeded_is_not_retryable() -> None:
    """Retrying a quota breach burns money and never succeeds."""
    err = ProviderError("over budget", provider_id="openai", kind=ErrorKind.QUOTA_EXCEEDED)
    assert err.is_retryable is False


def test_provider_error_carries_context_for_diagnostics() -> None:
    err = ProviderError(
        "429 slow down",
        provider_id="elevenlabs",
        kind=ErrorKind.RATE_LIMITED,
        retry_after_s=12.5,
    )
    assert err.provider_id == "elevenlabs"
    assert err.retry_after_s == 12.5
    assert "elevenlabs" in str(err)
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/core/test_errors.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'ytauto.core.errors'`

- [ ] **Step 3: Write the implementation**

`src/ytauto/core/errors.py`:

```python
"""Typed error taxonomy.

Every error raised anywhere in the application derives from ``YtautoError``.
Retry behaviour is derived from ``ErrorKind`` rather than decided at each call
site, so a provider cannot accidentally make a fatal error look retryable.
"""

from __future__ import annotations

from enum import StrEnum


class YtautoError(Exception):
    """Base class for every application error."""


class ConfigurationError(YtautoError):
    """The application is misconfigured; user action is required."""


class ValidationError(YtautoError):
    """Input failed a domain invariant."""


class ResourceExhausted(YtautoError):
    """A finite local resource (disk, VRAM, lease) was unavailable."""


class RenderError(YtautoError):
    """Video composition or export failed."""


class ErrorKind(StrEnum):
    """How the scheduler should treat a provider failure."""

    RETRYABLE = "retryable"
    FATAL = "fatal"
    RATE_LIMITED = "rate_limited"
    QUOTA_EXCEEDED = "quota_exceeded"


_RETRYABLE_KINDS = frozenset({ErrorKind.RETRYABLE, ErrorKind.RATE_LIMITED})


class ProviderError(YtautoError):
    """A provider call failed.

    ``kind`` drives scheduler behaviour. ``QUOTA_EXCEEDED`` is deliberately
    not retryable: retrying spends money and cannot succeed.
    """

    def __init__(
        self,
        message: str,
        *,
        provider_id: str,
        kind: ErrorKind,
        retry_after_s: float | None = None,
    ) -> None:
        super().__init__(f"[{provider_id}/{kind}] {message}")
        self.provider_id = provider_id
        self.kind = kind
        self.retry_after_s = retry_after_s

    @property
    def is_retryable(self) -> bool:
        return self.kind in _RETRYABLE_KINDS
```

- [ ] **Step 4: Run test to verify it passes**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/core/test_errors.py -v
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ytauto/core/errors.py tests/unit/core
git commit -m "feat: add typed error taxonomy with kind-derived retry semantics"
```

---

## Task 3: Application paths

**Files:**
- Create: `src/ytauto/infra/paths.py`
- Test: `tests/unit/infra/test_paths.py`

**Interfaces:**
- Consumes: `ytauto.core.errors.ConfigurationError`
- Produces: frozen dataclass `AppPaths` with fields `root, projects, cas, logs, cache, exports: Path` and `db_file: Path`; classmethod `AppPaths.resolve(override: Path | None = None) -> AppPaths`; method `ensure() -> None`.

Resolution order: explicit `override` argument → `YTAUTO_DATA_DIR` environment variable → `platformdirs.user_data_dir("ytauto", "ytauto")`. Every later task takes an `AppPaths` rather than computing paths itself — this is what keeps the app relocatable and packageable.

- [ ] **Step 1: Write the failing test**

`tests/unit/infra/test_paths.py` (create `tests/unit/infra/__init__.py` first):

```python
from pathlib import Path

import pytest

from ytauto.infra.paths import AppPaths


def test_explicit_override_wins(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path)
    assert paths.root == tmp_path


def test_env_var_is_used_when_no_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("YTAUTO_DATA_DIR", str(tmp_path))
    paths = AppPaths.resolve()
    assert paths.root == tmp_path


def test_falls_back_to_platform_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("YTAUTO_DATA_DIR", raising=False)
    paths = AppPaths.resolve()
    assert paths.root.is_absolute()
    assert "ytauto" in str(paths.root).lower()


def test_all_subpaths_live_under_root(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path)
    for child in (paths.projects, paths.cas, paths.logs, paths.cache, paths.exports):
        assert child.is_relative_to(tmp_path)
    assert paths.db_file.is_relative_to(tmp_path)


def test_ensure_creates_every_directory(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path / "fresh")
    paths.ensure()
    for child in (paths.projects, paths.cas, paths.logs, paths.cache, paths.exports):
        assert child.is_dir()


def test_ensure_is_idempotent(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path)
    paths.ensure()
    paths.ensure()
    assert paths.projects.is_dir()


def test_paths_are_frozen(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path)
    with pytest.raises(AttributeError):
        paths.root = tmp_path  # type: ignore[misc]
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_paths.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'ytauto.infra.paths'`

- [ ] **Step 3: Write the implementation**

`src/ytauto/infra/paths.py`:

```python
"""Filesystem layout for everything the application writes.

No module outside this one computes an application path. That rule is what
lets the data directory be relocated, tested against ``tmp_path``, and later
redirected by a packaged installer without touching call sites.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from platformdirs import user_data_dir

from ytauto.core.errors import ConfigurationError

_ENV_VAR = "YTAUTO_DATA_DIR"


@dataclass(frozen=True)
class AppPaths:
    """Resolved, absolute locations for application data."""

    root: Path
    projects: Path
    cas: Path
    logs: Path
    cache: Path
    exports: Path
    db_file: Path

    @classmethod
    def resolve(cls, override: Path | None = None) -> AppPaths:
        """Resolve the data root: explicit override, then env var, then platform default."""
        if override is not None:
            root = Path(override)
        elif (from_env := os.environ.get(_ENV_VAR)):
            root = Path(from_env)
        else:
            root = Path(user_data_dir(appname="ytauto", appauthor="ytauto"))

        root = root.expanduser().resolve()
        return cls(
            root=root,
            projects=root / "projects",
            cas=root / "assets" / "cas",
            logs=root / "logs",
            cache=root / "cache",
            exports=root / "exports",
            db_file=root / "ytauto.db",
        )

    def ensure(self) -> None:
        """Create every directory. Idempotent.

        Raises ConfigurationError if a directory cannot be created. An
        unwritable data root is a misconfiguration the user must resolve, not
        a transient fault — so it enters the typed taxonomy here rather than
        leaking a raw OSError to every caller.
        """
        for directory in (self.root, self.projects, self.cas, self.logs, self.cache, self.exports):
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise ConfigurationError(
                    f"cannot create application directory {directory}: {exc}"
                ) from exc
```

- [ ] **Step 4: Run test to verify it passes**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_paths.py -v
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ytauto/infra/paths.py tests/unit/infra
git commit -m "feat: add AppPaths with override/env/platform resolution"
```

---

## Task 4: Structured logging with correlation IDs

**Files:**
- Create: `src/ytauto/infra/logging.py`
- Test: `tests/unit/infra/test_logging.py`

**Interfaces:**
- Consumes: `ytauto.infra.paths.AppPaths`
- Produces:
  - `configure_logging(paths: AppPaths, *, level: str = "INFO") -> None`
  - `bind_correlation_id(cid: str | None = None) -> str` — sets and returns the current ID, generating a UUID4 hex when `None`
  - `current_correlation_id() -> str`
  - `get_logger(name: str) -> logging.Logger`
  - `JsonFormatter` — emits one JSON object per line

Correlation IDs use a `ContextVar` so a job's ID follows it across stage boundaries. Without this, diagnosing one failed video inside a 40-video batch means reading interleaved logs from concurrent workers.

- [ ] **Step 1: Write the failing test**

`tests/unit/infra/test_logging.py`:

```python
import json
import logging
from pathlib import Path

from ytauto.infra.logging import (
    JsonFormatter,
    bind_correlation_id,
    configure_logging,
    current_correlation_id,
    get_logger,
)
from ytauto.infra.paths import AppPaths


def _format_one(record: logging.LogRecord) -> dict[str, object]:
    return json.loads(JsonFormatter().format(record))


def _make_record(**extra: object) -> logging.LogRecord:
    record = logging.LogRecord(
        name="ytauto.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello %s",
        args=("world",),
        exc_info=None,
    )
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_formatter_emits_valid_json_with_required_fields() -> None:
    payload = _format_one(_make_record())
    assert payload["level"] == "INFO"
    assert payload["logger"] == "ytauto.test"
    assert payload["msg"] == "hello world"
    assert "ts" in payload


def test_formatter_includes_extra_fields() -> None:
    payload = _format_one(_make_record(stage="rewrite", project_id="abc"))
    assert payload["stage"] == "rewrite"
    assert payload["project_id"] == "abc"


def test_formatter_excludes_internal_logrecord_attributes() -> None:
    payload = _format_one(_make_record())
    for noisy in ("args", "msecs", "relativeCreated", "pathname", "exc_text"):
        assert noisy not in payload


def test_bind_correlation_id_generates_one_when_absent() -> None:
    cid = bind_correlation_id()
    assert len(cid) == 32
    assert current_correlation_id() == cid


def test_bind_correlation_id_accepts_explicit_value() -> None:
    bind_correlation_id("job-42")
    assert current_correlation_id() == "job-42"


def test_correlation_id_appears_in_formatted_output() -> None:
    bind_correlation_id("job-99")
    payload = _format_one(_make_record())
    assert payload["correlation_id"] == "job-99"


def test_configure_logging_writes_a_log_file(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path)
    paths.ensure()
    configure_logging(paths, level="DEBUG")
    try:
        get_logger("ytauto.test").info("written to disk", extra={"stage": "doctor"})
    finally:
        # Close only the handlers this test installed. logging.shutdown() would
        # close every handler in the interpreter, leaving closed-but-attached
        # handlers on the "ytauto" logger for the rest of the session.
        root = logging.getLogger("ytauto")
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()

    log_files = list(paths.logs.glob("*.jsonl"))
    assert log_files, "expected a .jsonl log file"
    lines = [json.loads(line) for line in log_files[0].read_text("utf-8").splitlines() if line]
    assert any(entry["msg"] == "written to disk" and entry["stage"] == "doctor" for entry in lines)
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_logging.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'ytauto.infra.logging'`

- [ ] **Step 3: Write the implementation**

`src/ytauto/infra/logging.py`:

```python
"""Structured JSON-lines logging with correlation-ID propagation."""

from __future__ import annotations

import json
import logging
import uuid
from contextvars import ContextVar
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from typing import Any

from ytauto.infra.paths import AppPaths

_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="-")

# Attributes the stdlib puts on every LogRecord. Anything else is caller context
# supplied via ``extra=`` and belongs in the emitted JSON.
_RESERVED: frozenset[str] = frozenset(
    {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "module", "msecs",
        "message", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "taskName", "thread", "threadName",
    }
)


def bind_correlation_id(cid: str | None = None) -> str:
    """Set the correlation ID for this context, generating one when omitted."""
    value = cid if cid is not None else uuid.uuid4().hex
    _correlation_id.set(value)
    return value


def current_correlation_id() -> str:
    return _correlation_id.get()


class JsonFormatter(logging.Formatter):
    """Render a LogRecord as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "correlation_id": current_correlation_id(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        return json.dumps(payload, default=str)


def configure_logging(paths: AppPaths, *, level: str = "INFO") -> None:
    """Install a rotating JSON-lines file handler and a plain console handler."""
    paths.ensure()
    root = logging.getLogger("ytauto")
    root.setLevel(level)
    # Close before dropping: list.clear() alone would leak the open log file
    # descriptor on every reconfiguration. On Windows that also locks the file,
    # so a later attempt to remove or rotate it fails with WinError 32.
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()
    root.propagate = False

    file_handler = RotatingFileHandler(
        paths.logs / "ytauto.jsonl",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(JsonFormatter())
    root.addHandler(file_handler)

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(levelname)-7s %(name)s: %(message)s"))
    root.addHandler(console)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
```

- [ ] **Step 4: Run test to verify it passes**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_logging.py -v
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ytauto/infra/logging.py tests/unit/infra/test_logging.py
git commit -m "feat: add JSON-lines logging with correlation-id propagation"
```

---

## Task 5: SQLite engine

**Files:**
- Create: `src/ytauto/infra/db/engine.py`
- Create: `src/ytauto/infra/clock.py`
- Test: `tests/unit/infra/test_db_engine.py`
- Test: `tests/unit/infra/test_clock.py`

**Interfaces:**
- Consumes: nothing beyond stdlib.
- Produces:
  - `connect(db_path: Path) -> sqlite3.Connection` — WAL, `foreign_keys=ON`, `busy_timeout=10000`, `synchronous=NORMAL`, `sqlite3.Row` row factory
  - `transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]` — context manager committing on success, rolling back on any exception
  - `utc_now_iso() -> str` in `clock.py` — ISO-8601 with explicit `+00:00` offset

WAL matters specifically here: the dispatcher writes job state while the GUI reads it. Without WAL those readers block writers and the UI stutters during batch runs.

`clock.py` is folded into this task because it is two functions and the first
consumer (migrations, Task 6) lands immediately after. Routing every timestamp
through one function is what makes the global "UTC, ISO-8601 with explicit
offset" constraint enforceable, and gives a single place to freeze time in
later tests. SQLite's `datetime('now')` is deliberately **not** used — it emits
a naive string with no offset, violating that constraint.

- [ ] **Step 1: Write the failing test**

`tests/unit/infra/test_db_engine.py`:

```python
import sqlite3
from pathlib import Path

import pytest

from ytauto.infra.db.engine import connect, transaction


def test_connection_uses_wal(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"
    conn.close()


def test_foreign_keys_are_enforced(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    conn.close()


def test_rows_are_accessible_by_column_name(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE t (a TEXT, b INTEGER)")
    conn.execute("INSERT INTO t VALUES ('x', 1)")
    row = conn.execute("SELECT * FROM t").fetchone()
    assert row["a"] == "x"
    assert row["b"] == 1
    conn.close()


def test_parent_directory_is_created(tmp_path: Path) -> None:
    conn = connect(tmp_path / "nested" / "deeper" / "t.db")
    conn.close()
    assert (tmp_path / "nested" / "deeper" / "t.db").exists()


def test_transaction_commits_on_success(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE t (a TEXT)")
    with transaction(conn):
        conn.execute("INSERT INTO t VALUES ('kept')")
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 1
    conn.close()


def test_transaction_rolls_back_on_error(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE t (a TEXT)")
    with pytest.raises(sqlite3.IntegrityError):
        with transaction(conn):
            conn.execute("INSERT INTO t VALUES ('gone')")
            raise sqlite3.IntegrityError("simulated failure")
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 0
    conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_db_engine.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'ytauto.infra.db.engine'`

- [ ] **Step 3: Write the implementation**

`src/ytauto/infra/db/engine.py`:

```python
"""SQLite connection factory and transaction helper.

WAL mode is required, not optional: the dispatcher writes job state while the
GUI reads it, and in rollback-journal mode those readers would block writers
and visibly stall the interface during batch runs.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

_PRAGMAS = (
    "PRAGMA journal_mode=WAL",
    "PRAGMA foreign_keys=ON",
    "PRAGMA busy_timeout=10000",
    "PRAGMA synchronous=NORMAL",
)


def connect(db_path: Path) -> sqlite3.Connection:
    """Open a connection with the pragmas this application requires."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    for pragma in _PRAGMAS:
        conn.execute(pragma)
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run a block in one transaction: commit on success, roll back on any error."""
    conn.execute("BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")
```

> **Nesting caveat for later phases:** `transaction()` issues a literal `BEGIN`,
> so nesting two of them on one connection raises
> "cannot start a transaction within a transaction". Nothing in Phase 0 nests.
> When Phase 1 adds the job queue, either keep transactions at the outermost
> call site or add savepoint support here.

- [ ] **Step 4: Run test to verify it passes**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_db_engine.py -v
```

Expected: 6 passed.

- [ ] **Step 5: Write the clock test**

`tests/unit/infra/test_clock.py`:

```python
from datetime import datetime

from ytauto.infra.clock import utc_now_iso


def test_returns_parseable_iso8601() -> None:
    parsed = datetime.fromisoformat(utc_now_iso())
    assert parsed.tzinfo is not None


def test_offset_is_explicitly_utc() -> None:
    value = utc_now_iso()
    assert value.endswith("+00:00"), value


def test_values_sort_chronologically_as_plain_strings() -> None:
    """CAS eviction orders by this column as TEXT, so string order must be time order."""
    first = utc_now_iso()
    second = utc_now_iso()
    assert first <= second
```

- [ ] **Step 6: Write the clock implementation**

`src/ytauto/infra/clock.py`:

```python
"""The single source of timestamps.

Every stored timestamp goes through here: UTC, ISO-8601, explicit offset.
Lexicographic string ordering of these values matches chronological ordering,
which the CAS evictor relies on when it sorts by ``last_accessed_at``.
"""

from __future__ import annotations

from datetime import UTC, datetime


def utc_now_iso() -> str:
    """Current UTC time as an ISO-8601 string ending in '+00:00'."""
    return datetime.now(tz=UTC).isoformat()
```

- [ ] **Step 7: Run the clock test to verify it passes**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_clock.py -v
```

Expected: 3 passed.

- [ ] **Step 8: Commit**

```bash
git add src/ytauto/infra/db/engine.py src/ytauto/infra/clock.py tests/unit/infra/test_db_engine.py tests/unit/infra/test_clock.py
git commit -m "feat: add SQLite engine with WAL pragmas, transaction helper, and UTC clock"
```

---

## Task 6: Schema migrations

**Files:**
- Create: `src/ytauto/infra/db/migrations.py`
- Test: `tests/unit/infra/test_migrations.py`

**Interfaces:**
- Consumes: `ytauto.infra.db.engine.transaction`, `ytauto.infra.clock.utc_now_iso`
- Produces:
  - frozen dataclass `Migration(version: int, name: str, statements: tuple[str, ...])`
  - `MIGRATIONS: tuple[Migration, ...]`
  - `current_version(conn) -> int` — `0` on a fresh database
  - `apply_migrations(conn) -> int` — applies pending migrations in order, returns the resulting version
  - `HEAD_VERSION: int`

Phase 0 ships exactly two tables — `cas_objects` and `settings`. Job and project tables arrive in Phase 1 as migration 002. Writing them now would be speculative.

**Why `statements: tuple[str, ...]` rather than one SQL blob.** The obvious
implementation wraps `conn.executescript(migration.sql)` in the `transaction()`
helper — and it is broken. Per the stdlib docs, `executescript` issues an
implicit `COMMIT` for any pending transaction before it runs, so the helper's
`BEGIN` is committed out from under it and the trailing `COMMIT` then raises
"cannot commit - no transaction is active". Splitting a blob on `;` is the
usual workaround and is fragile with string literals. Holding statements as a
tuple and executing them individually keeps each migration genuinely atomic
with no parsing at all.

- [ ] **Step 1: Write the failing test**

`tests/unit/infra/test_migrations.py`:

```python
import sqlite3
from pathlib import Path

import pytest

from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import (
    HEAD_VERSION,
    MIGRATIONS,
    Migration,
    apply_migrations,
    current_version,
)


def _tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"] for row in rows}


def test_fresh_database_is_at_version_zero(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    assert current_version(conn) == 0
    conn.close()


def test_apply_migrations_reaches_head(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    assert apply_migrations(conn) == HEAD_VERSION
    assert current_version(conn) == HEAD_VERSION
    conn.close()


def test_expected_tables_exist_after_migration(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    assert {"cas_objects", "settings", "schema_version"} <= _tables(conn)
    conn.close()


def test_apply_migrations_is_idempotent(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    before = _tables(conn)
    assert apply_migrations(conn) == HEAD_VERSION
    assert _tables(conn) == before
    conn.close()


def test_migrations_are_uniquely_and_contiguously_versioned() -> None:
    versions = [m.version for m in MIGRATIONS]
    assert versions == sorted(versions)
    assert len(set(versions)) == len(versions)
    assert versions == list(range(1, len(versions) + 1))


def test_cas_objects_rejects_duplicate_hashes(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    insert = (
        "INSERT INTO cas_objects (hash, kind, size_bytes, created_at, last_accessed_at) "
        "VALUES (?, ?, ?, ?, ?)"
    )
    args = ("a" * 64, "audio", 10, "2026-07-30T00:00:00+00:00", "2026-07-30T00:00:00+00:00")
    conn.execute(insert, args)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(insert, args)
    conn.close()


def test_failed_migration_rolls_back_schema_and_version_together(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The atomicity guarantee, asserted under an actual failure.

    Without this, every other test in this file would still pass if the
    `transaction()` wrapper were deleted outright — they only ever check
    post-success state.
    """
    conn = connect(tmp_path / "t.db")
    broken = Migration(
        version=1,
        name="broken",
        statements=(
            "CREATE TABLE will_not_survive (a TEXT)",
            "THIS IS NOT VALID SQL",
        ),
    )
    monkeypatch.setattr("ytauto.infra.db.migrations.MIGRATIONS", (broken,))

    with pytest.raises(sqlite3.OperationalError):
        apply_migrations(conn)

    assert current_version(conn) == 0, "version row must not survive a failed migration"
    assert "will_not_survive" not in _tables(conn), "DDL must roll back with the version row"
    conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_migrations.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'ytauto.infra.db.migrations'`

- [ ] **Step 3: Write the implementation**

`src/ytauto/infra/db/migrations.py`:

```python
"""Ordered, idempotent schema migrations.

Migrations are append-only. Never edit a released migration; add a new one.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import transaction


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    statements: tuple[str, ...]


_M001 = Migration(
    version=1,
    name="cas_and_settings",
    statements=(
        """
        CREATE TABLE cas_objects (
            hash             TEXT PRIMARY KEY,
            kind             TEXT NOT NULL,
            size_bytes       INTEGER NOT NULL,
            created_at       TEXT NOT NULL,
            last_accessed_at TEXT NOT NULL,
            refcount         INTEGER NOT NULL DEFAULT 0
        )
        """,
        "CREATE INDEX idx_cas_last_accessed ON cas_objects (last_accessed_at)",
        "CREATE INDEX idx_cas_refcount ON cas_objects (refcount)",
        """
        CREATE TABLE settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """,
    ),
)

MIGRATIONS: tuple[Migration, ...] = (_M001,)
HEAD_VERSION: int = MIGRATIONS[-1].version


def _ensure_version_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version    INTEGER PRIMARY KEY,
            name       TEXT NOT NULL,
            applied_at TEXT NOT NULL
        )
        """
    )


def current_version(conn: sqlite3.Connection) -> int:
    """Return the highest applied migration version, or 0 on a fresh database."""
    _ensure_version_table(conn)
    row = conn.execute("SELECT max(version) AS v FROM schema_version").fetchone()
    return int(row["v"]) if row["v"] is not None else 0


def apply_migrations(conn: sqlite3.Connection) -> int:
    """Apply every pending migration in order. Returns the resulting version.

    Each migration and its version row commit together, so a crash mid-migration
    can never leave the schema ahead of the recorded version.

    ``executescript`` is deliberately avoided: it implicitly commits any pending
    transaction, which would defeat that atomicity.
    """
    version = current_version(conn)
    for migration in MIGRATIONS:
        if migration.version <= version:
            continue
        with transaction(conn):
            for statement in migration.statements:
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_version (version, name, applied_at) VALUES (?, ?, ?)",
                (migration.version, migration.name, utc_now_iso()),
            )
        version = migration.version
    return version
```

- [ ] **Step 4: Run test to verify it passes**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_migrations.py -v
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ytauto/infra/db/migrations.py tests/unit/infra/test_migrations.py
git commit -m "feat: add idempotent schema migrations with cas_objects and settings"
```

---

## Task 7: Content-addressed store

**Files:**
- Create: `src/ytauto/infra/cas/store.py`
- Test: `tests/unit/infra/test_cas_store.py`

**Interfaces:**
- Consumes: `ytauto.core.errors.ValidationError`, `ytauto.infra.db.engine.transaction`, `ytauto.infra.clock.utc_now_iso`
- Produces:
  - `ContentHash = NewType("ContentHash", str)`
  - `hash_bytes(data: bytes) -> ContentHash` and `hash_file(path: Path) -> ContentHash` — pure
  - `class CasStore(root: Path, conn: sqlite3.Connection)` with:

    | Method | Signature |
    |---|---|
    | `put_bytes` | `(data: bytes, *, kind: str) -> ContentHash` |
    | `put_file` | `(src: Path, *, kind: str, move: bool = False) -> ContentHash` |
    | `path_for` | `(digest: ContentHash) -> Path` |
    | `exists` | `(digest: ContentHash) -> bool` |
    | `read_bytes` | `(digest: ContentHash) -> bytes` |
    | `retain` / `release` | `(digest: ContentHash) -> None` |
    | `refcount` | `(digest: ContentHash) -> int` |
    | `touch` | `(digest: ContentHash) -> None` |
    | `set_last_accessed` | `(digest: ContentHash, timestamp: str) -> None` |
    | `size_of` | `(digest: ContentHash) -> int` |
    | `total_size` | `() -> int` |
    | `iter_evictable` | `() -> list[tuple[ContentHash, int]]` |
    | `forget` | `(digest: ContentHash) -> None` |

Layout is `root/ab/cd/abcd…` — two levels of two-hex-char sharding, keeping directory entry counts manageable at hundreds of thousands of objects. Storing an object that already exists is a no-op that still refreshes `last_accessed_at`.

**`iter_evictable` and `forget` exist so the evictor never touches SQL.** Task 8
consumes only these two methods; the store remains the single owner of both the
`cas_objects` table and the on-disk layout. `set_last_accessed` is the public
seam that makes LRU ordering deterministic in tests without reaching into
private state — it is also what a future restore-from-backup path needs.

- [ ] **Step 1: Write the failing test**

`tests/unit/infra/test_cas_store.py`:

```python
import hashlib
from pathlib import Path

import pytest

from ytauto.core.errors import ValidationError
from ytauto.infra.cas.store import CasStore, hash_bytes, hash_file
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations


@pytest.fixture()
def store(tmp_path: Path) -> CasStore:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    return CasStore(root=tmp_path / "cas", conn=conn)


def test_hash_bytes_matches_sha256() -> None:
    assert hash_bytes(b"hello") == hashlib.sha256(b"hello").hexdigest()


def test_hash_is_full_length_lowercase_hex() -> None:
    digest = hash_bytes(b"anything")
    assert len(digest) == 64
    assert digest == digest.lower()


def test_hash_file_matches_hash_bytes(tmp_path: Path) -> None:
    f = tmp_path / "x.bin"
    f.write_bytes(b"payload")
    assert hash_file(f) == hash_bytes(b"payload")


def test_put_bytes_stores_and_returns_content(store: CasStore) -> None:
    digest = store.put_bytes(b"narration", kind="audio")
    assert store.exists(digest)
    assert store.read_bytes(digest) == b"narration"


def test_path_is_sharded_two_levels(store: CasStore) -> None:
    digest = store.put_bytes(b"x", kind="blob")
    path = store.path_for(digest)
    assert path.parent.name == digest[2:4]
    assert path.parent.parent.name == digest[0:2]


def test_identical_content_is_stored_once(store: CasStore) -> None:
    first = store.put_bytes(b"same", kind="audio")
    second = store.put_bytes(b"same", kind="audio")
    assert first == second
    assert store.total_size() == len(b"same")


def test_put_file_copies_by_default(store: CasStore, tmp_path: Path) -> None:
    src = tmp_path / "in.wav"
    src.write_bytes(b"wavdata")
    digest = store.put_file(src, kind="audio")
    assert src.exists(), "copy mode must leave the source in place"
    assert store.read_bytes(digest) == b"wavdata"


def test_put_file_with_move_removes_source(store: CasStore, tmp_path: Path) -> None:
    src = tmp_path / "in.wav"
    src.write_bytes(b"wavdata")
    store.put_file(src, kind="audio", move=True)
    assert not src.exists()


def test_refcount_starts_at_zero_and_tracks_retain_release(store: CasStore) -> None:
    digest = store.put_bytes(b"tracked", kind="blob")
    assert store.refcount(digest) == 0
    store.retain(digest)
    store.retain(digest)
    assert store.refcount(digest) == 2
    store.release(digest)
    assert store.refcount(digest) == 1


def test_release_never_goes_negative(store: CasStore) -> None:
    digest = store.put_bytes(b"floor", kind="blob")
    store.release(digest)
    assert store.refcount(digest) == 0


def test_total_size_sums_distinct_objects(store: CasStore) -> None:
    store.put_bytes(b"aaa", kind="blob")
    store.put_bytes(b"bbbb", kind="blob")
    assert store.total_size() == 7


def test_unknown_hash_raises_validation_error(store: CasStore) -> None:
    with pytest.raises(ValidationError):
        store.read_bytes("f" * 64)  # type: ignore[arg-type]


def test_malformed_hash_raises_validation_error(store: CasStore) -> None:
    with pytest.raises(ValidationError):
        store.path_for("not-a-hash")  # type: ignore[arg-type]


def test_iter_evictable_orders_least_recently_used_first(store: CasStore) -> None:
    recent = store.put_bytes(b"recent", kind="blob")
    stale = store.put_bytes(b"stale", kind="blob")
    store.set_last_accessed(recent, "2026-01-01T00:00:00+00:00")
    store.set_last_accessed(stale, "2020-01-01T00:00:00+00:00")

    order = [digest for digest, _size in store.iter_evictable()]

    assert order == [stale, recent]


def test_iter_evictable_excludes_retained_objects(store: CasStore) -> None:
    pinned = store.put_bytes(b"pinned", kind="blob")
    loose = store.put_bytes(b"loose", kind="blob")
    store.retain(pinned)

    assert [digest for digest, _size in store.iter_evictable()] == [loose]


def test_forget_removes_file_and_row(store: CasStore) -> None:
    digest = store.put_bytes(b"doomed", kind="blob")
    store.forget(digest)

    assert not store.exists(digest)
    with pytest.raises(ValidationError):
        store.refcount(digest)


def test_forget_is_idempotent(store: CasStore) -> None:
    digest = store.put_bytes(b"doomed", kind="blob")
    store.forget(digest)
    store.forget(digest)
    assert not store.exists(digest)
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_cas_store.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'ytauto.infra.cas.store'`

- [ ] **Step 3: Write the implementation**

`src/ytauto/infra/cas/store.py`:

```python
"""Content-addressed blob storage.

Objects are named by the SHA-256 of their contents, so identical bytes are
stored exactly once regardless of how many projects reference them. Refcounts
protect in-use objects from the evictor.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
from pathlib import Path
from typing import NewType

from ytauto.core.errors import ValidationError
from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import transaction

ContentHash = NewType("ContentHash", str)

_CHUNK = 1024 * 1024
_HEX = frozenset("0123456789abcdef")


def _validate(digest: str) -> ContentHash:
    if len(digest) != 64 or not set(digest) <= _HEX:
        raise ValidationError(f"not a valid sha256 hex digest: {digest!r}")
    return ContentHash(digest)


def hash_bytes(data: bytes) -> ContentHash:
    return ContentHash(hashlib.sha256(data).hexdigest())


def hash_file(path: Path) -> ContentHash:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
    return ContentHash(digest.hexdigest())


class CasStore:
    """Blob storage addressed by content hash, with refcounts held in SQLite."""

    def __init__(self, root: Path, conn: sqlite3.Connection) -> None:
        self._root = root
        self._conn = conn
        self._root.mkdir(parents=True, exist_ok=True)

    def path_for(self, digest: ContentHash) -> Path:
        valid = _validate(digest)
        return self._root / valid[0:2] / valid[2:4] / valid

    def exists(self, digest: ContentHash) -> bool:
        return self.path_for(digest).is_file()

    def _staging_path(self, target: Path) -> Path:
        """A tmp name unique per process.

        Phase 1 runs several worker subprocesses against this store; a shared
        ``.tmp`` name would let two of them corrupt each other's partial write.
        """
        return target.with_name(f"{target.name}.{os.getpid()}.tmp")

    def put_bytes(self, data: bytes, *, kind: str) -> ContentHash:
        digest = hash_bytes(data)
        target = self.path_for(digest)
        if not target.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._staging_path(target)
            tmp.write_bytes(data)
            tmp.replace(target)
        self._record(digest, kind=kind, size=len(data))
        return digest

    def put_file(self, src: Path, *, kind: str, move: bool = False) -> ContentHash:
        if not src.is_file():
            raise ValidationError(f"source file does not exist: {src}")
        digest = hash_file(src)
        target = self.path_for(digest)
        size = src.stat().st_size
        if target.is_file():
            if move:
                src.unlink()
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._staging_path(target)
            if move:
                shutil.move(str(src), tmp)
            else:
                shutil.copyfile(src, tmp)
            tmp.replace(target)
        self._record(digest, kind=kind, size=size)
        return digest

    def read_bytes(self, digest: ContentHash) -> bytes:
        path = self.path_for(digest)
        if not path.is_file():
            raise ValidationError(f"no such object in store: {digest}")
        return path.read_bytes()

    def _record(self, digest: ContentHash, *, kind: str, size: int) -> None:
        with transaction(self._conn):
            self._conn.execute(
                """
                INSERT INTO cas_objects (hash, kind, size_bytes, created_at, last_accessed_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(hash) DO UPDATE SET last_accessed_at = excluded.last_accessed_at
                """,
                (digest, kind, size, utc_now_iso(), utc_now_iso()),
            )

    def touch(self, digest: ContentHash) -> None:
        self.set_last_accessed(digest, utc_now_iso())

    def set_last_accessed(self, digest: ContentHash, timestamp: str) -> None:
        """Set the access time explicitly.

        Public because the evictor's LRU ordering must be assertable in tests,
        and because restore-from-backup needs to preserve original access times.
        """
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE cas_objects SET last_accessed_at = ? WHERE hash = ?", (timestamp, digest)
            )

    def retain(self, digest: ContentHash) -> None:
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE cas_objects SET refcount = refcount + 1 WHERE hash = ?", (digest,)
            )

    def release(self, digest: ContentHash) -> None:
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE cas_objects SET refcount = max(0, refcount - 1) WHERE hash = ?", (digest,)
            )

    def refcount(self, digest: ContentHash) -> int:
        row = self._conn.execute(
            "SELECT refcount FROM cas_objects WHERE hash = ?", (digest,)
        ).fetchone()
        if row is None:
            raise ValidationError(f"no such object in store: {digest}")
        return int(row["refcount"])

    def size_of(self, digest: ContentHash) -> int:
        row = self._conn.execute(
            "SELECT size_bytes FROM cas_objects WHERE hash = ?", (digest,)
        ).fetchone()
        if row is None:
            raise ValidationError(f"no such object in store: {digest}")
        return int(row["size_bytes"])

    def total_size(self) -> int:
        row = self._conn.execute("SELECT coalesce(sum(size_bytes), 0) AS s FROM cas_objects")
        return int(row.fetchone()["s"])

    def iter_evictable(self) -> list[tuple[ContentHash, int]]:
        """Unreferenced objects as (hash, size_bytes), least-recently-accessed first.

        Objects with refcount > 0 are excluded: they belong to a project or an
        in-flight job and must survive eviction.
        """
        rows = self._conn.execute(
            """
            SELECT hash, size_bytes FROM cas_objects
            WHERE refcount = 0
            ORDER BY last_accessed_at ASC
            """
        ).fetchall()
        return [(ContentHash(row["hash"]), int(row["size_bytes"])) for row in rows]

    def forget(self, digest: ContentHash) -> None:
        """Delete the object's file and its row. Idempotent."""
        self.path_for(digest).unlink(missing_ok=True)
        with transaction(self._conn):
            self._conn.execute("DELETE FROM cas_objects WHERE hash = ?", (digest,))
```

- [ ] **Step 4: Run test to verify it passes**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_cas_store.py -v
```

Expected: 17 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ytauto/infra/cas/store.py tests/unit/infra/test_cas_store.py
git commit -m "feat: add content-addressed store with sharding and refcounts"
```

---

## Task 8: Cache eviction

**Files:**
- Create: `src/ytauto/infra/cas/eviction.py`
- Test: `tests/unit/infra/test_cas_eviction.py`

**Interfaces:**
- Consumes: `ytauto.infra.cas.store.CasStore` — **only** its `total_size()`, `iter_evictable()` and `forget()` methods
- Produces:
  - frozen dataclass `EvictionPolicy(max_bytes: int)` with `EvictionPolicy.compute(cas_root: Path, current_size: int) -> EvictionPolicy`
  - frozen dataclass `EvictionReport(evicted: int, bytes_freed: int, bytes_remaining: int)`
  - `class Evictor(store: CasStore, policy: EvictionPolicy)` with `run() -> EvictionReport`
  - `MAX_CEILING_BYTES: int` = 40 GiB

Ceiling is `min(40 GiB, 40% of (free disk + bytes the cache already holds))`. Including the cache's own size makes the ceiling stable across runs — computing against raw free space alone makes the ceiling shrink every time the cache grows, which oscillates.

**Objects with `refcount > 0` are never evicted.** They belong to a project or an in-flight job.

- [ ] **Step 1: Write the failing test**

`tests/unit/infra/test_cas_eviction.py`:

```python
from pathlib import Path

import pytest

from ytauto.core.errors import ValidationError
from ytauto.infra.cas.eviction import MAX_CEILING_BYTES, Evictor, EvictionPolicy
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations


@pytest.fixture()
def store(tmp_path: Path) -> CasStore:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    return CasStore(root=tmp_path / "cas", conn=conn)


OLD = "2020-01-01T00:00:00+00:00"
NEW = "2026-01-01T00:00:00+00:00"


def test_ceiling_is_capped_at_40_gib(tmp_path: Path) -> None:
    policy = EvictionPolicy.compute(tmp_path, current_size=0)
    assert policy.max_bytes <= MAX_CEILING_BYTES


def test_ceiling_is_positive(tmp_path: Path) -> None:
    assert EvictionPolicy.compute(tmp_path, current_size=0).max_bytes > 0


def test_nothing_evicted_when_under_ceiling(store: CasStore) -> None:
    store.put_bytes(b"small", kind="blob")
    report = Evictor(store, EvictionPolicy(max_bytes=1_000_000)).run()
    assert report.evicted == 0
    assert report.bytes_freed == 0


def test_evicts_least_recently_used_first(store: CasStore) -> None:
    old = store.put_bytes(b"0123456789", kind="blob")   # 10 bytes
    new = store.put_bytes(b"abcdefghij", kind="blob")   # 10 bytes
    store.set_last_accessed(old, OLD)
    store.set_last_accessed(new, NEW)

    report = Evictor(store, EvictionPolicy(max_bytes=10)).run()

    assert report.evicted == 1
    assert not store.exists(old)
    assert store.exists(new)


def test_retained_objects_are_never_evicted(store: CasStore) -> None:
    pinned = store.put_bytes(b"0123456789", kind="blob")
    store.retain(pinned)
    store.set_last_accessed(pinned, OLD)

    report = Evictor(store, EvictionPolicy(max_bytes=0)).run()

    assert report.evicted == 0
    assert store.exists(pinned)


def test_stops_as_soon_as_it_is_under_the_ceiling(store: CasStore) -> None:
    """Eviction must free just enough, not empty the cache."""
    oldest = store.put_bytes(b"0123456789", kind="blob")
    middle = store.put_bytes(b"abcdefghij", kind="blob")
    newest = store.put_bytes(b"klmnopqrst", kind="blob")
    store.set_last_accessed(oldest, "2020-01-01T00:00:00+00:00")
    store.set_last_accessed(middle, "2023-01-01T00:00:00+00:00")
    store.set_last_accessed(newest, NEW)

    report = Evictor(store, EvictionPolicy(max_bytes=20)).run()

    assert report.evicted == 1
    assert not store.exists(oldest)
    assert store.exists(middle)
    assert store.exists(newest)


def test_report_totals_are_accurate(store: CasStore) -> None:
    a = store.put_bytes(b"0123456789", kind="blob")
    store.put_bytes(b"abcdefghij", kind="blob")
    store.set_last_accessed(a, OLD)

    report = Evictor(store, EvictionPolicy(max_bytes=10)).run()

    assert report.bytes_freed == 10
    assert report.bytes_remaining == 10
    assert store.total_size() == 10


def test_database_rows_are_removed_with_the_files(store: CasStore) -> None:
    digest = store.put_bytes(b"0123456789", kind="blob")
    store.set_last_accessed(digest, OLD)

    Evictor(store, EvictionPolicy(max_bytes=0)).run()

    assert not store.exists(digest)
    with pytest.raises(ValidationError):
        store.refcount(digest)
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_cas_eviction.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'ytauto.infra.cas.eviction'`

- [ ] **Step 3: Write the implementation**

`src/ytauto/infra/cas/eviction.py`:

```python
"""LRU eviction for the content-addressed store.

The target machine has ~84 GB free. Without a ceiling and an evictor, batch
operation fills the disk within weeks. Objects with refcount > 0 belong to a
project or an in-flight job and are never evicted.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from ytauto.infra.cas.store import CasStore

MAX_CEILING_BYTES = 40 * 1024**3
_FREE_FRACTION = 0.40


@dataclass(frozen=True)
class EvictionPolicy:
    max_bytes: int

    @classmethod
    def compute(cls, cas_root: Path, current_size: int) -> EvictionPolicy:
        """Ceiling = min(40 GiB, 40% of (free space + what the cache already holds)).

        Including ``current_size`` keeps the ceiling stable as the cache grows;
        computing against raw free space alone makes it shrink on every run.
        """
        cas_root.mkdir(parents=True, exist_ok=True)
        free = shutil.disk_usage(cas_root).free
        budget = int((free + current_size) * _FREE_FRACTION)
        return cls(max_bytes=max(1, min(MAX_CEILING_BYTES, budget)))


@dataclass(frozen=True)
class EvictionReport:
    evicted: int
    bytes_freed: int
    bytes_remaining: int


class Evictor:
    def __init__(self, store: CasStore, policy: EvictionPolicy) -> None:
        self._store = store
        self._policy = policy

    def run(self) -> EvictionReport:
        """Evict least-recently-used unreferenced objects until under the ceiling."""
        total = self._store.total_size()
        if total <= self._policy.max_bytes:
            return EvictionReport(evicted=0, bytes_freed=0, bytes_remaining=total)

        freed = 0
        evicted = 0
        for digest, size in self._store.iter_evictable():
            if total - freed <= self._policy.max_bytes:
                break
            self._store.forget(digest)
            freed += size
            evicted += 1

        return EvictionReport(evicted=evicted, bytes_freed=freed, bytes_remaining=total - freed)
```

The evictor owns *retention policy* and nothing else — it issues no SQL and
knows nothing about the on-disk layout. Both remain private to `CasStore`, so
changing the storage layout later touches exactly one file.

- [ ] **Step 4: Run test to verify it passes**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_cas_eviction.py -v
```

Expected: 8 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ytauto/infra/cas/eviction.py tests/unit/infra/test_cas_eviction.py
git commit -m "feat: add LRU cache eviction with refcount protection"
```

---

## Task 9: FFmpeg locator

**Files:**
- Create: `src/ytauto/infra/ffmpeg/locator.py`
- Test: `tests/unit/infra/test_ffmpeg_locator.py`

**Interfaces:**
- Consumes: `ytauto.core.errors.ConfigurationError`
- Produces:
  - frozen dataclass `FfmpegBinaries(ffmpeg: Path, ffprobe: Path, version: str)`
  - `parse_version(banner: str) -> str` — **pure**
  - `locate(configured_dir: Path | None = None) -> FfmpegBinaries`
  - `FfmpegNotFound(ConfigurationError)`

Search order: `configured_dir` → `YTAUTO_FFMPEG_DIR` env var → `PATH` → the bundled directory beside the package (Phase 9). `ffprobe` must sit next to `ffmpeg`; a mismatched pair is a common and confusing failure, so it is detected explicitly.

- [ ] **Step 1: Write the failing test**

`tests/unit/infra/test_ffmpeg_locator.py`:

```python
from pathlib import Path

import pytest

from ytauto.infra.ffmpeg.locator import FfmpegNotFound, locate, parse_version

BANNER = (
    "ffmpeg version 7.1.1-essentials_build-www.gyan.dev Copyright (c) 2000-2025 the FFmpeg "
    "developers\nbuilt with gcc 14.2.0 (Rev1, Built by MSYS2 project)\n"
)


def test_parse_version_extracts_the_version_token() -> None:
    assert parse_version(BANNER) == "7.1.1-essentials_build-www.gyan.dev"


def test_parse_version_handles_a_plain_release_banner() -> None:
    assert parse_version("ffmpeg version 6.0 Copyright (c) 2000-2023") == "6.0"


def test_parse_version_returns_unknown_when_unparseable() -> None:
    assert parse_version("not an ffmpeg banner at all") == "unknown"


def _fake_pair(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "ffmpeg.exe").write_text("stub")
    (directory / "ffprobe.exe").write_text("stub")


def test_configured_directory_is_preferred(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_pair(tmp_path / "bin")
    monkeypatch.setattr(
        "ytauto.infra.ffmpeg.locator._read_version", lambda _: "7.1.1"
    )
    found = locate(configured_dir=tmp_path / "bin")
    assert found.ffmpeg.parent == tmp_path / "bin"
    assert found.ffprobe.exists()
    assert found.version == "7.1.1"


def test_env_var_is_used_when_no_argument(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_pair(tmp_path / "envbin")
    monkeypatch.setenv("YTAUTO_FFMPEG_DIR", str(tmp_path / "envbin"))
    monkeypatch.setattr("ytauto.infra.ffmpeg.locator._read_version", lambda _: "7.1.1")
    assert locate().ffmpeg.parent == tmp_path / "envbin"


def test_missing_ffprobe_beside_ffmpeg_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lonely = tmp_path / "lonely"
    lonely.mkdir()
    (lonely / "ffmpeg.exe").write_text("stub")
    monkeypatch.delenv("YTAUTO_FFMPEG_DIR", raising=False)
    monkeypatch.setattr("shutil.which", lambda *_a, **_k: None)
    with pytest.raises(FfmpegNotFound, match="ffprobe"):
        locate(configured_dir=lonely)


def test_raises_when_nothing_is_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("YTAUTO_FFMPEG_DIR", raising=False)
    monkeypatch.setattr("shutil.which", lambda *_a, **_k: None)
    with pytest.raises(FfmpegNotFound):
        locate(configured_dir=tmp_path / "nonexistent")
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_ffmpeg_locator.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'ytauto.infra.ffmpeg.locator'`

- [ ] **Step 3: Write the implementation**

`src/ytauto/infra/ffmpeg/locator.py`:

```python
"""Locate the ffmpeg/ffprobe binaries.

Version parsing is a pure function so it is testable without executing
anything. Only ``_read_version`` touches a subprocess.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from ytauto.core.errors import ConfigurationError

_ENV_VAR = "YTAUTO_FFMPEG_DIR"
_VERSION_RE = re.compile(r"^ffmpeg version (\S+)")
_EXE = ".exe" if os.name == "nt" else ""


class FfmpegNotFound(ConfigurationError):
    """Neither a configured, environment, nor PATH installation was usable."""


@dataclass(frozen=True)
class FfmpegBinaries:
    ffmpeg: Path
    ffprobe: Path
    version: str


def parse_version(banner: str) -> str:
    """Extract the version token from an ffmpeg banner. Pure."""
    match = _VERSION_RE.search(banner.strip())
    return match.group(1) if match else "unknown"


def _read_version(ffmpeg: Path) -> str:
    result = subprocess.run(  # noqa: S603
        [str(ffmpeg), "-hide_banner", "-version"],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    return parse_version(result.stdout)


def _pair_in(directory: Path) -> tuple[Path, Path] | None:
    ffmpeg = directory / f"ffmpeg{_EXE}"
    if not ffmpeg.is_file():
        return None
    ffprobe = directory / f"ffprobe{_EXE}"
    if not ffprobe.is_file():
        raise FfmpegNotFound(
            f"found ffmpeg at {ffmpeg} but no ffprobe beside it; "
            "install a complete build so the pair stays version-matched"
        )
    return ffmpeg, ffprobe


def locate(configured_dir: Path | None = None) -> FfmpegBinaries:
    """Find a matched ffmpeg/ffprobe pair. Raises FfmpegNotFound if unavailable."""
    candidates: list[Path] = []
    if configured_dir is not None:
        candidates.append(Path(configured_dir))
    if from_env := os.environ.get(_ENV_VAR):
        candidates.append(Path(from_env))
    if on_path := shutil.which("ffmpeg"):
        candidates.append(Path(on_path).parent)
    candidates.append(Path(__file__).resolve().parents[3] / "bin")

    for directory in candidates:
        if not directory.is_dir():
            continue
        if (pair := _pair_in(directory)) is not None:
            ffmpeg, ffprobe = pair
            return FfmpegBinaries(ffmpeg=ffmpeg, ffprobe=ffprobe, version=_read_version(ffmpeg))

    raise FfmpegNotFound(
        "ffmpeg was not found. Install it and put it on PATH, or set "
        f"{_ENV_VAR} to the directory containing ffmpeg and ffprobe."
    )
```

- [ ] **Step 4: Run test to verify it passes**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_ffmpeg_locator.py -v
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ytauto/infra/ffmpeg/locator.py tests/unit/infra/test_ffmpeg_locator.py
git commit -m "feat: add ffmpeg/ffprobe locator with pure version parsing"
```

---

## Task 10: FFmpeg capability probe

**Files:**
- Create: `src/ytauto/infra/ffmpeg/probe.py`
- Test: `tests/unit/infra/test_ffmpeg_probe.py`

**Interfaces:**
- Consumes: `ytauto.infra.ffmpeg.locator.FfmpegBinaries`, `ytauto.core.errors.ConfigurationError`
- Produces:
  - `parse_encoders(output: str) -> frozenset[str]` — **pure**
  - `parse_filters(output: str) -> frozenset[str]` — **pure**
  - frozen dataclass `FfmpegCapabilities(encoders, filters: frozenset[str])` with
    `best_h264_encoder() -> str` and `has_subtitle_burn_in() -> bool`
  - `probe(binaries: FfmpegBinaries) -> FfmpegCapabilities`
  - `ENCODER_PREFERENCE: tuple[str, ...]` = `("h264_nvenc", "h264_qsv", "libx264")`

The two parsers are pure, so the whole selection policy is tested against captured real output with no subprocess in the loop. `has_subtitle_burn_in()` matters because a build without `libass` cannot render captions at all — that must fail loudly in `doctor`, not silently at render time.

- [ ] **Step 1: Write the failing test**

`tests/unit/infra/test_ffmpeg_probe.py`:

```python
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


def test_nvenc_is_preferred_when_available() -> None:
    caps = FfmpegCapabilities(
        encoders=parse_encoders(ENCODERS_OUTPUT), filters=parse_filters(FILTERS_OUTPUT)
    )
    assert caps.best_h264_encoder() == "h264_nvenc"


def test_falls_back_to_qsv_without_nvenc() -> None:
    caps = FfmpegCapabilities(
        encoders=frozenset({"h264_qsv", "libx264"}), filters=frozenset()
    )
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
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_ffmpeg_probe.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'ytauto.infra.ffmpeg.probe'`

- [ ] **Step 3: Write the implementation**

`src/ytauto/infra/ffmpeg/probe.py`:

```python
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

# ffmpeg lists capabilities as: six flag characters, whitespace, then the name.
_ENCODER_RE = re.compile(r"^\s*[A-Z.]{6}\s+(\S+)")
_FILTER_RE = re.compile(r"^\s*[A-Z.]{3}\s+(\S+)")


def parse_encoders(output: str) -> frozenset[str]:
    """Extract encoder names from ``ffmpeg -encoders`` output. Pure."""
    return frozenset(
        match.group(1)
        for line in output.splitlines()
        if (match := _ENCODER_RE.match(line))
    )


def parse_filters(output: str) -> frozenset[str]:
    """Extract filter names from ``ffmpeg -filters`` output. Pure."""
    return frozenset(
        match.group(1)
        for line in output.splitlines()
        if (match := _FILTER_RE.match(line))
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
```

- [ ] **Step 4: Run test to verify it passes**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_ffmpeg_probe.py -v
```

Expected: 8 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ytauto/infra/ffmpeg/probe.py tests/unit/infra/test_ffmpeg_probe.py
git commit -m "feat: add ffmpeg capability probe with pure output parsers"
```

---

## Task 11: GPU detection

**Files:**
- Create: `src/ytauto/infra/gpu.py`
- Test: `tests/unit/infra/test_gpu.py`

**Interfaces:**
- Consumes: nothing beyond stdlib.
- Produces:
  - frozen dataclass `GpuInfo(name: str, vram_mb: int, driver: str)`
  - `parse_nvidia_smi(csv_output: str) -> GpuInfo | None` — **pure**
  - `detect() -> GpuInfo | None` — returns `None` when no NVIDIA GPU is present

`detect()` returning `None` is a normal outcome, not an error: the app must run on machines without an NVIDIA card by falling back to QSV or libx264. Phase 1's resource governor sizes the `gpu_compute` pool from `vram_mb`.

- [ ] **Step 1: Write the failing test**

`tests/unit/infra/test_gpu.py`:

```python
from ytauto.infra.gpu import GpuInfo, detect, parse_nvidia_smi

# Captured verbatim from the target machine.
SMI_OUTPUT = "NVIDIA GeForce RTX 3050 Laptop GPU, 4096 MiB, 592.82\n"


def test_parses_name_vram_and_driver() -> None:
    info = parse_nvidia_smi(SMI_OUTPUT)
    assert info == GpuInfo(name="NVIDIA GeForce RTX 3050 Laptop GPU", vram_mb=4096, driver="592.82")


def test_parses_vram_without_a_unit_suffix() -> None:
    info = parse_nvidia_smi("NVIDIA A100, 40960, 550.54\n")
    assert info is not None
    assert info.vram_mb == 40960


def test_uses_the_first_gpu_when_several_are_listed() -> None:
    info = parse_nvidia_smi("GPU One, 4096 MiB, 592.82\nGPU Two, 8192 MiB, 592.82\n")
    assert info is not None
    assert info.name == "GPU One"


def test_empty_output_yields_none() -> None:
    assert parse_nvidia_smi("") is None
    assert parse_nvidia_smi("   \n  ") is None


def test_malformed_output_yields_none_rather_than_raising() -> None:
    assert parse_nvidia_smi("something went wrong") is None
    assert parse_nvidia_smi("GPU, not-a-number, 1.0") is None


def test_detect_returns_gpuinfo_or_none() -> None:
    """Runs against the real machine; both outcomes are valid."""
    result = detect()
    assert result is None or isinstance(result, GpuInfo)
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_gpu.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'ytauto.infra.gpu'`

- [ ] **Step 3: Write the implementation**

`src/ytauto/infra/gpu.py`:

```python
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
    """Detect the first NVIDIA GPU, or None when nvidia-smi is absent or fails."""
    executable = shutil.which("nvidia-smi")
    if executable is None:
        return None
    try:
        result = subprocess.run(  # noqa: S603
            [executable, _QUERY, _FORMAT],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return parse_nvidia_smi(result.stdout)
```

- [ ] **Step 4: Run test to verify it passes**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_gpu.py -v
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ytauto/infra/gpu.py tests/unit/infra/test_gpu.py
git commit -m "feat: add NVIDIA GPU detection with pure output parsing"
```

---

## Task 12: `ytauto doctor`

**Files:**
- Create: `src/ytauto/cli/doctor.py`
- Create: `src/ytauto/cli/__main__.py`
- Test: `tests/unit/cli/test_doctor.py`

**Interfaces:**
- Consumes: `AppPaths`, `connect`, `apply_migrations`, `HEAD_VERSION`, `CasStore`, `EvictionPolicy`, `locate`, `probe`, `FfmpegNotFound`, `gpu.detect`, `ConfigurationError`, `configure_logging`, `bind_correlation_id`
- Produces:
  - `Severity` — `StrEnum` with `OK`, `WARN`, `FAIL`
  - frozen dataclass `CheckResult(name: str, severity: Severity, detail: str)`
  - `run_checks(paths: AppPaths) -> list[CheckResult]`
  - `format_report(results: list[CheckResult]) -> str`
  - `exit_code(results: list[CheckResult]) -> int` — `1` if any `FAIL`, else `0`
  - `main(argv: list[str] | None = None) -> int` in `__main__.py`

**This task is the phase exit criterion.** `WARN` versus `FAIL` matters: a missing GPU is a warning (libx264 still works), while a missing `ass` filter is a failure (no video can ever be subtitled).

- [ ] **Step 1: Write the failing test**

`tests/unit/cli/test_doctor.py` (create `tests/unit/cli/__init__.py` first):

```python
from pathlib import Path

import pytest

from ytauto.cli.doctor import (
    CheckResult,
    Severity,
    exit_code,
    format_report,
    run_checks,
)
from ytauto.infra.paths import AppPaths


def test_exit_code_is_zero_when_all_ok() -> None:
    results = [CheckResult("a", Severity.OK, "fine"), CheckResult("b", Severity.WARN, "meh")]
    assert exit_code(results) == 0


def test_exit_code_is_one_when_anything_failed() -> None:
    results = [CheckResult("a", Severity.OK, "fine"), CheckResult("b", Severity.FAIL, "broken")]
    assert exit_code(results) == 1


def test_warnings_alone_do_not_fail_the_run() -> None:
    """A machine without an NVIDIA GPU is degraded, not broken."""
    assert exit_code([CheckResult("gpu", Severity.WARN, "none found")]) == 0


def test_format_report_includes_every_check_name_and_detail() -> None:
    text = format_report(
        [CheckResult("python", Severity.OK, "3.12.1"), CheckResult("gpu", Severity.WARN, "absent")]
    )
    assert "python" in text
    assert "3.12.1" in text
    assert "gpu" in text
    assert "absent" in text


def test_run_checks_covers_the_expected_surface(tmp_path: Path) -> None:
    results = run_checks(AppPaths.resolve(override=tmp_path))
    names = {r.name for r in results}
    assert {
        "python",
        "data directories",
        "database",
        "cache ceiling",
        "ffmpeg",
        "h264 encoder",
        "subtitle burn-in",
        "gpu",
        "free disk",
    } <= names


def test_run_checks_creates_a_usable_database(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path)
    run_checks(paths)
    assert paths.db_file.exists()


def test_python_check_passes_on_the_supported_interpreter(tmp_path: Path) -> None:
    results = run_checks(AppPaths.resolve(override=tmp_path))
    python = next(r for r in results if r.name == "python")
    assert python.severity is Severity.OK


def test_missing_ffmpeg_is_reported_as_a_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ytauto.infra.ffmpeg.locator import FfmpegNotFound

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise FfmpegNotFound("simulated absence")

    monkeypatch.setattr("ytauto.cli.doctor.locate", _boom)
    results = run_checks(AppPaths.resolve(override=tmp_path))
    ffmpeg = next(r for r in results if r.name == "ffmpeg")
    assert ffmpeg.severity is Severity.FAIL
    assert exit_code(results) == 1
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/cli/test_doctor.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'ytauto.cli.doctor'`

- [ ] **Step 3: Write the doctor implementation**

`src/ytauto/cli/doctor.py`:

```python
"""Environment checks. The Phase 0 exit criterion.

Severity is meaningful: WARN means degraded but usable (no NVIDIA GPU - fall
back to libx264), FAIL means a required capability is absent (no libass means
no video can ever be subtitled).
"""

from __future__ import annotations

import shutil
import sqlite3
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
        conn = connect(paths.db_file)
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
        conn.close()
    except (OSError, sqlite3.Error) as exc:
        results.append(CheckResult("database", Severity.FAIL, str(exc)))
        results.append(CheckResult("cache ceiling", Severity.FAIL, "unavailable"))
    return results


def _check_ffmpeg() -> list[CheckResult]:
    try:
        binaries = locate()
    except FfmpegNotFound as exc:
        return [
            CheckResult("ffmpeg", Severity.FAIL, str(exc)),
            CheckResult("h264 encoder", Severity.FAIL, "ffmpeg unavailable"),
            CheckResult("subtitle burn-in", Severity.FAIL, "ffmpeg unavailable"),
        ]

    results = [CheckResult("ffmpeg", Severity.OK, f"{binaries.version} at {binaries.ffmpeg}")]
    capabilities = probe(binaries)
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
```

- [ ] **Step 4: Write the CLI entry point**

`src/ytauto/cli/__main__.py`:

```python
"""Command-line entry point."""

from __future__ import annotations

import argparse
from pathlib import Path

from ytauto import __version__
from ytauto.cli.doctor import exit_code, format_report, run_checks
from ytauto.infra.logging import bind_correlation_id, configure_logging
from ytauto.infra.paths import AppPaths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ytauto", description="Faceless video automation")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--data-dir", type=Path, default=None, help="override the data directory")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("doctor", help="check that the environment is usable")

    args = parser.parse_args(argv)
    paths = AppPaths.resolve(override=args.data_dir)
    configure_logging(paths)
    bind_correlation_id()

    if args.command == "doctor":
        results = run_checks(paths)
        print(format_report(results))
        return exit_code(results)

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 5: Run tests to verify they pass**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/cli/test_doctor.py -v
```

Expected: 8 passed.

- [ ] **Step 6: Run the real doctor — this is the phase exit criterion**

```powershell
.\.venv\Scripts\ytauto.exe doctor
```

Expected output resembling:

```
[ OK ]  python             3.12.x
[ OK ]  data directories   C:\Users\noobz\AppData\Local\ytauto\ytauto
[ OK ]  database           schema v1 (head v1)
[ OK ]  cache ceiling      0.00 GB used of 33.6 GB
[ OK ]  ffmpeg             7.1.1-essentials_build-www.gyan.dev at C:\ffmpeg-...\ffmpeg.exe
[ OK ]  h264 encoder       h264_nvenc
[ OK ]  subtitle burn-in   libass available
[ OK ]  gpu                NVIDIA GeForce RTX 3050 Laptop GPU, 4096 MiB, driver 592.82
[ OK ]  free disk          84.2 GB free

environment is GREEN
```

Confirm the exit code:

```powershell
.\.venv\Scripts\ytauto.exe doctor; $LASTEXITCODE
```

Expected: `0`

- [ ] **Step 7: Run the full quality gate**

```powershell
powershell -ExecutionPolicy Bypass -File scripts/check.ps1
```

Expected: `ALL CHECKS PASSED`

- [ ] **Step 8: Commit**

```bash
git add src/ytauto/cli tests/unit/cli
git commit -m "feat: add ytauto doctor environment check command"
```

---

## Phase 0 Exit Checklist

- [ ] `ytauto doctor` prints `environment is GREEN` and exits `0`
- [ ] `scripts/check.ps1` passes: ruff, mypy, import-linter, pytest
- [ ] `import-linter` proves `core/` imports no `infra`, `app`, `providers`, `ui`, or `PySide6`
- [ ] Every module has tests; every text parser is pure and tested against captured real output
- [ ] No `TODO` or `FIXME` on the shipped path
- [ ] Migrations apply idempotently from a fresh database

**Next:** Phase 1 (domain models, ports, `Stage` protocol, DAG, fingerprinting, job queue, resource governor, worker protocol) gets its own plan, written once Phase 0 is green.
