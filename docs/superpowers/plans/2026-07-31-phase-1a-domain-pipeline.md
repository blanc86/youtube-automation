# Phase 1a: Domain + Pipeline Framework — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the pure domain layer and the content-addressed pipeline framework — models, `Stage`, fingerprinting, DAG, ports, artifact store — so that a pipeline can be defined, fingerprinted, and asked "what actually needs to rerun?" with no execution machinery in existence yet.

**Architecture:** Everything new in `core/` is pure Python over the standard library, enforced by the existing import-linter contract. The one impure addition, `ArtifactStore`, lives in `infra/` and is the bridge between stage fingerprints and the Phase 0 content-addressed store. Three Phase 0 debts that Phase 1b's runtime depends on are cleared first, because retrofitting them under a live job queue is materially harder.

**Tech Stack:** Python 3.12, stdlib only for `core/` (`hashlib`, `json`, `enum`, `dataclasses`, `typing`), `sqlite3` for `infra/`. No new third-party dependencies.

## Global Constraints

Every task's requirements implicitly include this section.

- **`core/` imports nothing but the standard library.** No `infra`, `app`, `providers`, `ui`, `cli`, no `PySide6`, no third-party packages. `import-linter` fails the build on violation.
- **`ytauto.core.*` must pass `mypy --strict`.** Complete annotations on every function, parameter and return.
- **No bare `except`.** Every handler names concrete types.
- **Every public function and method in `core/**` and `infra/**` carries a `Raises:` docstring section** naming concrete exception types and when — or says nothing if it genuinely raises nothing. This is the convention established at the end of Phase 0 and it is the thing that stops the next caller guessing wrong.
- **Content hashes are SHA-256, lowercase hex, full 64 chars**, via `ytauto.core.models.content_hash`.
- **All timestamps UTC ISO-8601 with explicit `+00:00`**, via `ytauto.infra.clock.utc_now_iso()`. SQLite's `datetime('now')` is forbidden.
- **Fingerprints must be stable across processes and interpreter restarts.** No `hash()`, no `id()`, no reliance on dict insertion order, no absolute paths.
- Migrations are **append-only**. Never edit a released migration; add a new one.
- Test database connections must be closed so Windows can delete `tmp_path`.
- Test output pristine — no warnings.
- Run `.\.venv\Scripts\python.exe -m ruff format src tests` before the gate; `scripts/check.ps1` genuinely fails on any step's non-zero exit.
- **When a test's purpose is to pin a guard** — a `try/except`, a filter, a transaction wrapper, a validation branch — demonstrate it failing with the production guard deleted, not merely with the feature unimplemented. Phase 0 shipped four tests that could not fail for their own reason; this rule is why.

Specs of record: `docs/superpowers/specs/2026-07-30-youtube-automation-design.md` (§3.2 ports, §3.4 pipeline, §5.1–5.3, §6) and `docs/superpowers/phase-0-carry-forward.md`.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/ytauto/infra/db/engine.py` *(modify)* | Add `immediate=` to `transaction()` — deferred `BEGIN` breaks read-then-write claims |
| `src/ytauto/infra/logging.py` *(modify)* | Stamp correlation ID at emission, not format time; make file logging optional |
| `src/ytauto/infra/cas/eviction.py` *(modify)* | Reclaim orphan blobs and abandoned staging files |
| `src/ytauto/infra/cas/store.py` *(modify)* | `forget()` deletes the row before unlinking; expose `known_digests()` |
| `src/ytauto/infra/db/migrations.py` *(modify)* | Migration 002: `jobs`, `job_stages`, `artifacts` |
| `src/ytauto/core/models/artifact.py` | `ArtifactRef` — the unit a stage produces |
| `src/ytauto/core/models/job.py` | `JobState`, `StageStatus` — lifecycle vocabulary |
| `src/ytauto/core/pipeline/stage.py` | `Stage` protocol, `JobContext`, `StageResult`, `ProgressFn` |
| `src/ytauto/core/pipeline/fingerprint.py` | Canonical JSON + `compute_fingerprint` |
| `src/ytauto/core/pipeline/graph.py` | `Pipeline` — validation, topological order, downstream invalidation |
| `src/ytauto/core/ports/capability.py` | `CapabilityDescriptor`, `CostModel`, `LatencyClass` |
| `src/ytauto/core/ports/*.py` | The seven provider `Protocol`s + reserved `Publisher` |
| `src/ytauto/infra/artifacts.py` | `ArtifactStore` — fingerprint → artifacts, backed by CAS + SQLite |

**Why `ArtifactStore` is in `infra/` and not `core/`:** it performs SQLite and filesystem I/O. `core/pipeline/` names fingerprints and `ArtifactRef`s only; Phase 1b's runner is what wires the two together.

---

## Task 1: `BEGIN IMMEDIATE` for read-then-write transactions

**Files:**
- Modify: `src/ytauto/infra/db/engine.py`
- Test: `tests/unit/infra/test_db_engine.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `transaction(conn: sqlite3.Connection, *, immediate: bool = False) -> Iterator[sqlite3.Connection]`

**Why this is first.** `transaction()` issues a deferred `BEGIN`. Every current caller's first statement is a write, so the write lock is taken immediately and `busy_timeout=10000` applies. Phase 1b's queue claim and governor lease are **read-then-write**. In WAL mode, a deferred transaction that reads and *then* writes while another connection has written gets `SQLITE_BUSY_SNAPSHOT` returned **immediately — the busy handler is never invoked**, so the 10-second timeout does nothing. It surfaces as a flaky, load-dependent queue bug. Cheaper to add now than to diagnose later.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/infra/test_db_engine.py`:

```python
def test_immediate_takes_the_write_lock_before_any_statement(tmp_path: Path) -> None:
    """An immediate transaction locks on BEGIN, so a read-then-write claim is safe."""
    db = tmp_path / "t.db"
    writer = connect(db)
    writer.execute("CREATE TABLE t (a TEXT)")
    other = connect(db)
    other.execute("PRAGMA busy_timeout=0")

    with transaction(writer, immediate=True):
        with pytest.raises(sqlite3.OperationalError, match="locked|busy"):
            other.execute("INSERT INTO t VALUES ('blocked')")

    other.close()
    writer.close()


def test_deferred_does_not_hold_the_lock_until_its_first_write(tmp_path: Path) -> None:
    """The contrast that makes the previous test meaningful."""
    db = tmp_path / "t.db"
    reader = connect(db)
    reader.execute("CREATE TABLE t (a TEXT)")
    other = connect(db)
    other.execute("PRAGMA busy_timeout=0")

    with transaction(reader):
        reader.execute("SELECT count(*) FROM t").fetchone()
        other.execute("INSERT INTO t VALUES ('allowed')")

    assert other.execute("SELECT count(*) FROM t").fetchone()[0] == 1
    other.close()
    reader.close()


def test_immediate_still_commits_and_rolls_back(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE t (a TEXT)")

    with transaction(conn, immediate=True):
        conn.execute("INSERT INTO t VALUES ('kept')")
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 1

    with pytest.raises(sqlite3.IntegrityError), transaction(conn, immediate=True):
        conn.execute("INSERT INTO t VALUES ('gone')")
        raise sqlite3.IntegrityError("simulated")
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 1
    conn.close()
```

- [ ] **Step 2: Run to verify they fail**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_db_engine.py -k immediate -v
```

Expected: FAIL — `transaction() got an unexpected keyword argument 'immediate'`.

- [ ] **Step 3: Implement**

In `src/ytauto/infra/db/engine.py`, change the `transaction` signature and its first statement:

```python
@contextmanager
def transaction(
    conn: sqlite3.Connection, *, immediate: bool = False
) -> Iterator[sqlite3.Connection]:
    """Run a block in one transaction: commit on success, roll back on any error.

    Pass ``immediate=True`` for read-then-write work such as claiming a queued
    job or acquiring a resource lease. A deferred ``BEGIN`` upgrades to a write
    lock lazily, and in WAL mode that upgrade returns SQLITE_BUSY_SNAPSHOT
    *immediately* without invoking the busy handler - so ``busy_timeout`` does
    not apply and the caller sees a spurious failure under concurrency.

    Nesting two transactions on one connection raises OperationalError; keep
    transactions at the outermost call site.

    Raises:
        sqlite3.OperationalError: if a transaction is already open on ``conn``
            (a programming error), or if the write lock could not be acquired
            within ``busy_timeout`` (legitimate contention - callers competing
            for a job or a lease must expect and handle this).
        BaseException: anything raised inside the block, after rolling back.
    """
    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")
```

- [ ] **Step 4: Run to verify they pass**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_db_engine.py -v
```

Expected: 9 passed.

- [ ] **Step 5: Prove the guard is load-bearing**

Temporarily change `"BEGIN IMMEDIATE" if immediate else "BEGIN"` to just `"BEGIN"`. Confirm `test_immediate_takes_the_write_lock_before_any_statement` FAILS. Restore, confirm PASS. Paste both outputs in your report.

- [ ] **Step 6: Commit**

```bash
git add src/ytauto/infra/db/engine.py tests/unit/infra/test_db_engine.py
git commit -m "feat: add immediate mode to transaction for read-then-write claims"
```

---

## Task 2: Correlation IDs that survive the process boundary

**Files:**
- Modify: `src/ytauto/infra/logging.py`
- Test: `tests/unit/infra/test_logging.py`

**Interfaces:**
- Consumes: `ytauto.infra.paths.AppPaths`
- Produces: `configure_logging(paths: AppPaths, *, level: str = "INFO", file_logging: bool = True) -> None`; `CorrelationIdFilter`

**The bug being prevented.** `JsonFormatter` currently calls `current_correlation_id()` at **format** time. Once Phase 1b's workers report logs through a pipe and the parent re-emits them, every relayed line gets stamped with the *parent's* ID — silently destroying the per-job trail that is the entire point. Stamping at emission, and only when the record does not already carry one, fixes it.

`file_logging=False` exists because concurrent `RotatingFileHandler` rollover across processes fails on Windows with `WinError 32`. Workers must never own a file handler.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/infra/test_logging.py`:

```python
def test_a_record_keeps_its_own_correlation_id() -> None:
    """A relayed worker record must not be restamped with the parent's ID."""
    bind_correlation_id("parent-job")
    record = _make_record(correlation_id="worker-job")
    CorrelationIdFilter().filter(record)
    assert json.loads(JsonFormatter().format(record))["correlation_id"] == "worker-job"


def test_a_record_without_an_id_gets_the_current_context_id() -> None:
    bind_correlation_id("ambient-job")
    record = _make_record()
    CorrelationIdFilter().filter(record)
    assert json.loads(JsonFormatter().format(record))["correlation_id"] == "ambient-job"


def test_formatter_falls_back_when_the_filter_never_ran() -> None:
    """Direct formatter use (as in these tests) must not raise."""
    bind_correlation_id("fallback-job")
    assert json.loads(JsonFormatter().format(_make_record()))["correlation_id"] == "fallback-job"


def test_file_logging_can_be_disabled(tmp_path: Path) -> None:
    """Workers must not own a rotating file handler - concurrent rollover
    fails on Windows with WinError 32."""
    paths = AppPaths.resolve(override=tmp_path)
    paths.ensure()
    configure_logging(paths, file_logging=False)
    try:
        handlers = logging.getLogger("ytauto").handlers
        assert not any(isinstance(h, RotatingFileHandler) for h in handlers)
        assert handlers, "console logging must still be installed"
        assert not list(paths.logs.glob("*.jsonl"))
    finally:
        root = logging.getLogger("ytauto")
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
```

Add `from logging.handlers import RotatingFileHandler` and `CorrelationIdFilter` to the module's imports.

- [ ] **Step 2: Run to verify they fail**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_logging.py -v
```

Expected: FAIL — `cannot import name 'CorrelationIdFilter'`.

- [ ] **Step 3: Implement**

In `src/ytauto/infra/logging.py`, add the filter, have the formatter prefer the record's own value, and add the parameter:

```python
class CorrelationIdFilter(logging.Filter):
    """Stamp the current correlation ID onto records that lack one.

    Stamping happens at emission rather than at format time so that records
    relayed from a worker subprocess keep the ID they were created with. A
    formatter that read the ContextVar directly would overwrite every relayed
    line with the parent process's ID.

    Attach this to HANDLERS, never to the logger. ``Logger.filter()`` runs only
    inside ``Logger.handle()``, i.e. only for records logged through that exact
    logger object. A record from a child logger such as
    ``ytauto.core.pipeline`` reaches this logger's handlers through
    ``callHandlers()``, which walks ancestors' ``.handlers`` and never consults
    their ``.filters`` - so a logger-attached filter silently never fires for
    any real call site.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "correlation_id"):
            record.correlation_id = current_correlation_id()
        return True
```

In `JsonFormatter.format`, replace the `correlation_id` line:

```python
            "correlation_id": getattr(record, "correlation_id", current_correlation_id()),
```

`correlation_id` is already excluded from the extras loop because it is now a real record attribute — add it to `_RESERVED` so it is not emitted twice.

Then in `configure_logging`:

```python
def configure_logging(
    paths: AppPaths, *, level: str = "INFO", file_logging: bool = True
) -> None:
    """Install log handlers on the ``ytauto`` logger.

    Pass ``file_logging=False`` in worker subprocesses: concurrent
    RotatingFileHandler rollover across processes fails on Windows with
    WinError 32. Workers report through the pipe and the parent writes the file.

    Raises:
        ConfigurationError: if the data directories cannot be created.
        OSError: if the log file cannot be opened.
    """
    root = logging.getLogger("ytauto")
    root.setLevel(level)
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()
    root.filters.clear()
    root.propagate = False

    if file_logging:
        paths.ensure()
        file_handler = RotatingFileHandler(
            paths.logs / "ytauto.jsonl", maxBytes=10 * 1024 * 1024, backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(JsonFormatter())
        file_handler.addFilter(CorrelationIdFilter())
        root.addHandler(file_handler)

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(levelname)-7s %(name)s: %(message)s"))
    console.addFilter(CorrelationIdFilter())
    root.addHandler(console)
```

Note `paths.ensure()` moved inside the `file_logging` branch — a worker with no file handler has no reason to require a writable data root.

- [ ] **Step 4: Run to verify they pass**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_logging.py -v
```

Expected: 12 passed.

- [ ] **Step 5: Prove the guard is load-bearing**

Temporarily delete the `if not hasattr(record, "correlation_id"):` condition so the filter always overwrites. Confirm `test_a_record_keeps_its_own_correlation_id` FAILS. Restore, confirm PASS. Paste both outputs.

- [ ] **Step 6: Commit**

```bash
git add src/ytauto/infra/logging.py tests/unit/infra/test_logging.py
git commit -m "feat: stamp correlation ids at emission and make file logging optional"
```

---

## Task 3: Reclaim CAS orphans and abandoned staging files

**Files:**
- Modify: `src/ytauto/infra/cas/store.py`
- Modify: `src/ytauto/infra/cas/eviction.py`
- Modify: `tests/unit/infra/conftest.py`
- Test: `tests/unit/infra/test_cas_eviction.py`

**First, split the shared fixture** so tests can reach the database without
touching `CasStore._conn`. Phase 0's review flagged private access in the
evictor and it was fixed there; these tests must not reintroduce it, and Task 10
needs the same connection to build an `ArtifactStore`. Replace the body of
`tests/unit/infra/conftest.py` with:

```python
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations


@pytest.fixture()
def db_conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """A migrated database. Closed on teardown so Windows can delete tmp_path."""
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def store(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    """A CasStore sharing the migrated connection from ``db_conn``."""
    return CasStore(root=tmp_path / "cas", conn=db_conn)
```

Existing tests that request only `store` keep working unchanged.

**Interfaces:**
- Consumes: `CasStore`
- Produces: `CasStore.known_digests() -> frozenset[ContentHash]`; `SweepReport(orphan_blobs: int, orphan_bytes: int, stale_staging: int, stale_staging_bytes: int)`; `Evictor.sweep_orphans(*, min_age_s: float = 900.0) -> SweepReport`

**The leak being closed.** `Evictor.run()` reads database rows only, so two kinds of garbage are unreclaimable forever: a blob whose file landed but whose row did not, and `{digest}.{pid}.tmp` files left by a killed worker. The spec plans explicitly for worker death, so on an 84 GB disk under batch operation this leaks monotonically.

**`min_age_s` is a correctness requirement, not tuning.** `put_bytes` writes the file and *then* records the row. A blob written microseconds ago looks exactly like an orphan. Only sweeping files older than the threshold makes the sweep safe to run concurrently with writes.

Also change `forget()` to delete the row **before** unlinking: a crash between the two then leaves a reclaimable orphan rather than a phantom row that makes `total_size()` overcount and `read_bytes()` fail for a digest `size_of()` happily answers.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/infra/test_cas_eviction.py`:

```python
import os
import time


def test_sweep_removes_a_blob_with_no_row(
    store: CasStore, db_conn: sqlite3.Connection
) -> None:
    digest = store.put_bytes(b"orphaned", kind="blob")
    path = store.path_for(digest)
    db_conn.execute("DELETE FROM cas_objects WHERE hash = ?", (digest,))
    _age_file(path, seconds=3600)

    report = Evictor(store, EvictionPolicy(max_bytes=10**9)).sweep_orphans()

    assert report.orphan_blobs == 1
    assert report.orphan_bytes == len(b"orphaned")
    assert not path.exists()


def test_sweep_keeps_blobs_that_have_rows(store: CasStore) -> None:
    digest = store.put_bytes(b"legitimate", kind="blob")
    _age_file(store.path_for(digest), seconds=3600)

    report = Evictor(store, EvictionPolicy(max_bytes=10**9)).sweep_orphans()

    assert report.orphan_blobs == 0
    assert store.exists(digest)


def test_sweep_spares_recently_written_blobs(
    store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """put_bytes writes the file before recording the row; a blob written
    microseconds ago is indistinguishable from an orphan."""
    digest = store.put_bytes(b"just written", kind="blob")
    path = store.path_for(digest)
    db_conn.execute("DELETE FROM cas_objects WHERE hash = ?", (digest,))

    report = Evictor(store, EvictionPolicy(max_bytes=10**9)).sweep_orphans()

    assert report.orphan_blobs == 0, "a fresh blob must not be swept"
    assert path.exists()


def test_sweep_removes_stale_staging_files(store: CasStore) -> None:
    shard = store.path_for(("a" * 64)).parent  # type: ignore[arg-type]
    shard.mkdir(parents=True, exist_ok=True)
    stale = shard / f"{'a' * 64}.99999.tmp"
    stale.write_bytes(b"partial")
    _age_file(stale, seconds=3600)

    report = Evictor(store, EvictionPolicy(max_bytes=10**9)).sweep_orphans()

    assert report.stale_staging == 1
    assert report.stale_staging_bytes == len(b"partial")
    assert not stale.exists()


def test_sweep_spares_recent_staging_files(store: CasStore) -> None:
    """A live worker's in-progress write must survive a concurrent sweep."""
    shard = store.path_for(("b" * 64)).parent  # type: ignore[arg-type]
    shard.mkdir(parents=True, exist_ok=True)
    fresh = shard / f"{'b' * 64}.{os.getpid()}.tmp"
    fresh.write_bytes(b"in progress")

    report = Evictor(store, EvictionPolicy(max_bytes=10**9)).sweep_orphans()

    assert report.stale_staging == 0
    assert fresh.exists()


def test_forget_deletes_the_row_before_unlinking(
    store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """Ordering matters: a crash between the two must leave a reclaimable
    orphan, never a phantom row that makes total_size() overcount."""
    digest = store.put_bytes(b"doomed", kind="blob")
    observed: list[bool] = []
    original_unlink = Path.unlink

    def _spy(self: Path, *args: object, **kwargs: object) -> None:
        observed.append(
            db_conn.execute(
                "SELECT 1 FROM cas_objects WHERE hash = ?", (digest,)
            ).fetchone()
            is None
        )
        original_unlink(self, *args, **kwargs)  # type: ignore[arg-type]

    with patch.object(Path, "unlink", _spy):
        store.forget(digest)

    assert observed == [True], "row must already be gone when unlink runs"
```

Add this helper near the top of the file, plus `import sqlite3`, `from pathlib import Path` and `from unittest.mock import patch`:

```python
def _age_file(path: Path, *, seconds: float) -> None:
    """Backdate a file's mtime so age-based sweeping is deterministic."""
    past = time.time() - seconds
    os.utime(path, (past, past))
```

- [ ] **Step 2: Run to verify they fail**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_cas_eviction.py -k "sweep or forget_deletes" -v
```

Expected: FAIL — `Evictor` has no attribute `sweep_orphans`.

- [ ] **Step 3: Add `known_digests` and reorder `forget` in `store.py`**

```python
    def known_digests(self) -> frozenset[ContentHash]:
        """Every digest with a row in ``cas_objects``.

        Used by the evictor's orphan sweep to tell recorded blobs from garbage.
        """
        rows = self._conn.execute("SELECT hash FROM cas_objects").fetchall()
        return frozenset(ContentHash(row["hash"]) for row in rows)
```

Replace the body of `forget`:

```python
    def forget(self, digest: ContentHash) -> None:
        """Delete the object's row and then its file. Idempotent.

        The row goes first on purpose: a crash between the two steps leaves a
        file with no row, which the orphan sweep reclaims. The reverse order
        would leave a row with no file - which makes total_size() overcount and
        read_bytes() fail for a digest size_of() still answers.

        Raises:
            ValidationError: if ``digest`` is not a valid sha256 hex digest.
            OSError: if the file exists but cannot be removed.
        """
        path = self.path_for(digest)
        with transaction(self._conn):
            self._conn.execute("DELETE FROM cas_objects WHERE hash = ?", (digest,))
        path.unlink(missing_ok=True)
```

- [ ] **Step 4: Implement the sweep in `eviction.py`**

```python
_STAGING_SUFFIX = ".tmp"
_DEFAULT_MIN_AGE_S = 900.0


@dataclass(frozen=True)
class SweepReport:
    orphan_blobs: int
    orphan_bytes: int
    stale_staging: int
    stale_staging_bytes: int
    phantom_rows: int
```

Add to `Evictor`:

```python
    def sweep_orphans(self, *, min_age_s: float = _DEFAULT_MIN_AGE_S) -> SweepReport:
        """Reclaim blobs with no row and staging files from dead workers.

        ``min_age_s`` is a correctness requirement, not tuning: put_bytes writes
        the file before recording the row, so a blob written moments ago is
        indistinguishable from an orphan. Only files older than the threshold
        are touched, which makes this safe to run while workers are writing.

        Raises:
            OSError: if the cache directory cannot be walked.
        """
        known = self._store.known_digests()
        cutoff = time.time() - min_age_s
        orphans = orphan_bytes = staging = staging_bytes = 0

        for path in self._store.root.glob("*/*/*"):
            if not path.is_file():
                continue
            try:
                stat = path.stat()
            except FileNotFoundError:
                continue  # a concurrent sweep or writer got there first
            if stat.st_mtime > cutoff:
                continue

            if path.name.endswith(_STAGING_SUFFIX):
                path.unlink(missing_ok=True)
                staging += 1
                staging_bytes += stat.st_size
            elif path.name not in known and not self._store.has_row(path.name):
                # Re-check immediately before unlinking. `known` is a snapshot,
                # and put_bytes is idempotent: a worker storing identical
                # content between the snapshot and here sees the file already
                # present, skips the write, and records a row - after which
                # deleting the file would strand that row.
                path.unlink(missing_ok=True)
                orphans += 1
                orphan_bytes += stat.st_size

        phantoms = self._store.forget_rows_without_files()

        return SweepReport(
            orphan_blobs=orphans,
            orphan_bytes=orphan_bytes,
            stale_staging=staging,
            stale_staging_bytes=staging_bytes,
            phantom_rows=phantoms,
        )
```

Add `import time` and expose the store's root — add this property to `CasStore`:

```python
    @property
    def root(self) -> Path:
        """The content-addressed store's base directory."""
        return self._root
```

- [ ] **Step 5: Run to verify they pass**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_cas_eviction.py -v
```

Expected: 14 passed.

- [ ] **Step 6: Prove the age guard is load-bearing**

Temporarily delete the `if stat.st_mtime > cutoff: continue` lines. Confirm `test_sweep_spares_recently_written_blobs` and `test_sweep_spares_recent_staging_files` both FAIL. Restore, confirm PASS. Paste both outputs.

- [ ] **Step 7: Commit**

```bash
git add src/ytauto/infra/cas tests/unit/infra/test_cas_eviction.py
git commit -m "feat: reclaim cas orphans and abandoned staging files"
```

---

## Task 4: Migration 002 — jobs, stages, artifacts

**Files:**
- Modify: `src/ytauto/infra/db/migrations.py`
- Test: `tests/unit/infra/test_migrations.py`

**Interfaces:**
- Consumes: `Migration`, `apply_migrations`, `HEAD_VERSION`
- Produces: `HEAD_VERSION == 2`; tables `jobs`, `job_stages`, `artifacts`

Phase 1b writes to `jobs` and `job_stages`; Task 10 writes to `artifacts`. The schema lands now so both build against a fixed shape.

`artifacts` is keyed by `(fingerprint, name)` because one stage can emit several outputs — `synthesize_speech` produces both narration audio and word boundaries.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/infra/test_migrations.py`:

```python
def test_head_is_version_two(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    assert apply_migrations(conn) == 2
    assert HEAD_VERSION == 2
    conn.close()


def test_phase_one_tables_exist(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    assert {"jobs", "job_stages", "artifacts"} <= _tables(conn)
    conn.close()


def test_job_stages_cascade_when_a_job_is_deleted(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    now = "2026-07-31T00:00:00+00:00"
    conn.execute(
        "INSERT INTO jobs (id, project_id, pipeline_id, state, created_at, updated_at) "
        "VALUES ('j1', 'p1', 'shorts', 'queued', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO job_stages (job_id, stage_id, status) VALUES ('j1', 'rewrite', 'pending')"
    )
    conn.execute("DELETE FROM jobs WHERE id = 'j1'")
    assert conn.execute("SELECT count(*) FROM job_stages").fetchone()[0] == 0
    conn.close()


def test_artifacts_allow_several_outputs_per_fingerprint(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    insert = (
        "INSERT INTO artifacts (fingerprint, name, stage_id, kind, digest, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    now = "2026-07-31T00:00:00+00:00"
    conn.execute(insert, ("f" * 64, "narration", "tts", "audio", "a" * 64, now))
    conn.execute(insert, ("f" * 64, "timings", "tts", "json", "b" * 64, now))
    assert conn.execute("SELECT count(*) FROM artifacts").fetchone()[0] == 2

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(insert, ("f" * 64, "narration", "tts", "audio", "c" * 64, now))
    conn.close()


def test_migration_002_is_applied_on_top_of_an_existing_001(tmp_path: Path) -> None:
    """Upgrade path, not just a fresh create."""
    db = tmp_path / "t.db"
    conn = connect(db)
    monkeyed = MIGRATIONS[:1]
    with patch("ytauto.infra.db.migrations.MIGRATIONS", monkeyed):
        assert apply_migrations(conn) == 1
    assert "jobs" not in _tables(conn)

    assert apply_migrations(conn) == 2
    assert {"cas_objects", "jobs"} <= _tables(conn)
    conn.close()
```

Add `from unittest.mock import patch` to the test module.

- [ ] **Step 2: Run to verify they fail**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_migrations.py -v
```

Expected: FAIL — `assert 1 == 2` on head version.

- [ ] **Step 3: Implement**

Add to `src/ytauto/infra/db/migrations.py`, after `_M001`:

```python
_M002 = Migration(
    version=2,
    name="jobs_stages_artifacts",
    statements=(
        """
        CREATE TABLE jobs (
            id               TEXT PRIMARY KEY,
            project_id       TEXT NOT NULL,
            pipeline_id      TEXT NOT NULL,
            state            TEXT NOT NULL,
            priority         INTEGER NOT NULL DEFAULT 0,
            attempts         INTEGER NOT NULL DEFAULT 0,
            created_at       TEXT NOT NULL,
            updated_at       TEXT NOT NULL,
            lease_owner      TEXT,
            lease_expires_at TEXT,
            last_error       TEXT
        )
        """,
        "CREATE INDEX idx_jobs_claimable ON jobs (state, priority DESC, created_at)",
        "CREATE INDEX idx_jobs_lease ON jobs (lease_expires_at)",
        """
        CREATE TABLE job_stages (
            job_id      TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            stage_id    TEXT NOT NULL,
            status      TEXT NOT NULL,
            fingerprint TEXT,
            started_at  TEXT,
            finished_at TEXT,
            error       TEXT,
            PRIMARY KEY (job_id, stage_id)
        )
        """,
        """
        CREATE TABLE artifacts (
            fingerprint TEXT NOT NULL,
            name        TEXT NOT NULL,
            stage_id    TEXT NOT NULL,
            kind        TEXT NOT NULL,
            digest      TEXT NOT NULL,
            meta_json   TEXT NOT NULL DEFAULT '{}',
            created_at  TEXT NOT NULL,
            PRIMARY KEY (fingerprint, name)
        )
        """,
        "CREATE INDEX idx_artifacts_digest ON artifacts (digest)",
    ),
)

MIGRATIONS: tuple[Migration, ...] = (_M001, _M002)
```

- [ ] **Step 4: Run to verify they pass**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_migrations.py -v
```

Expected: 12 passed.

- [ ] **Step 5: Confirm `doctor` reports the new head**

```powershell
.\.venv\Scripts\ytauto.exe doctor
```

The `database` row must read `schema v2 (head v2)` and stay `[ OK ]`. Paste the output.

- [ ] **Step 6: Commit**

```bash
git add src/ytauto/infra/db/migrations.py tests/unit/infra/test_migrations.py
git commit -m "feat: add migration 002 with jobs, job_stages and artifacts"
```

---

## Task 5: Domain vocabulary — `ArtifactRef`, `JobState`, `StageStatus`

**Files:**
- Create: `src/ytauto/core/models/artifact.py`
- Create: `src/ytauto/core/models/job.py`
- Test: `tests/unit/core/test_artifact.py`, `tests/unit/core/test_job.py`

**Interfaces:**
- Consumes: `ytauto.core.models.content_hash.ContentHash`, `validate_digest`; `ytauto.core.errors.ValidationError`
- Produces:
  - `ArtifactRef(name: str, kind: str, digest: ContentHash)` — frozen, validating
  - `JobState` StrEnum: `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`; `JobState.is_terminal: bool`
  - `StageStatus` StrEnum: `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `SKIPPED`; `StageStatus.is_done: bool`

`SKIPPED` is the fingerprint-hit status and it counts as done — that distinction is what makes resume and cheap iteration work.

- [ ] **Step 1: Write the failing tests**

`tests/unit/core/test_artifact.py`:

```python
import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import hash_bytes


def test_holds_name_kind_and_digest() -> None:
    digest = hash_bytes(b"narration")
    ref = ArtifactRef(name="narration", kind="audio", digest=digest)
    assert (ref.name, ref.kind, ref.digest) == ("narration", "audio", digest)


def test_is_frozen() -> None:
    ref = ArtifactRef(name="n", kind="audio", digest=hash_bytes(b"x"))
    with pytest.raises(AttributeError):
        ref.name = "other"  # type: ignore[misc]


def test_rejects_a_malformed_digest() -> None:
    with pytest.raises(ValidationError):
        ArtifactRef(name="n", kind="audio", digest="not-a-hash")  # type: ignore[arg-type]


def test_rejects_an_empty_name() -> None:
    with pytest.raises(ValidationError, match="name"):
        ArtifactRef(name="", kind="audio", digest=hash_bytes(b"x"))


def test_rejects_an_empty_kind() -> None:
    with pytest.raises(ValidationError, match="kind"):
        ArtifactRef(name="n", kind="", digest=hash_bytes(b"x"))


def test_equal_refs_are_interchangeable() -> None:
    digest = hash_bytes(b"same")
    assert ArtifactRef("n", "audio", digest) == ArtifactRef("n", "audio", digest)
    assert len({ArtifactRef("n", "audio", digest), ArtifactRef("n", "audio", digest)}) == 1
```

`tests/unit/core/test_job.py`:

```python
import pytest

from ytauto.core.models.job import JobState, StageStatus


@pytest.mark.parametrize(
    ("state", "terminal"),
    [
        (JobState.QUEUED, False),
        (JobState.RUNNING, False),
        (JobState.SUCCEEDED, True),
        (JobState.FAILED, True),
        (JobState.CANCELLED, True),
    ],
)
def test_job_terminality(state: JobState, terminal: bool) -> None:
    assert state.is_terminal is terminal


@pytest.mark.parametrize(
    ("status", "done"),
    [
        (StageStatus.PENDING, False),
        (StageStatus.RUNNING, False),
        (StageStatus.SUCCEEDED, True),
        (StageStatus.SKIPPED, True),
        (StageStatus.FAILED, False),
    ],
)
def test_stage_doneness(status: StageStatus, done: bool) -> None:
    """SKIPPED is a fingerprint cache hit - done, and not to be rerun.
    FAILED is NOT done: resume must retry it."""
    assert status.is_done is done


def test_states_serialise_as_plain_strings() -> None:
    """These values are persisted in SQLite TEXT columns."""
    assert f"{JobState.QUEUED}" == "queued"
    assert f"{StageStatus.SKIPPED}" == "skipped"
```

- [ ] **Step 2: Run to verify they fail**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/core -v
```

Expected: FAIL — `No module named 'ytauto.core.models.artifact'`.

- [ ] **Step 3: Implement**

`src/ytauto/core/models/artifact.py`:

```python
"""The unit of output a pipeline stage produces."""

from __future__ import annotations

from dataclasses import dataclass

from ytauto.core.errors import ValidationError
from ytauto.core.models.content_hash import ContentHash, validate_digest


@dataclass(frozen=True)
class ArtifactRef:
    """A named, content-addressed output of a stage.

    Holds a digest rather than bytes: artifacts can be gigabytes of video, and
    the pipeline passes references between stages, never payloads.

    Raises:
        ValidationError: if ``name`` or ``kind`` is empty, or ``digest`` is not
            a valid sha256 hex digest.
    """

    name: str
    kind: str
    digest: ContentHash

    def __post_init__(self) -> None:
        if not self.name:
            raise ValidationError("artifact name must not be empty")
        if not self.kind:
            raise ValidationError("artifact kind must not be empty")
        validate_digest(self.digest)
```

`src/ytauto/core/models/job.py`:

```python
"""Lifecycle vocabulary for jobs and their stages."""

from __future__ import annotations

from enum import StrEnum


class JobState(StrEnum):
    """Where a job sits in its lifecycle. Persisted as TEXT."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """True when no further work will happen without explicit requeueing."""
        return self in _TERMINAL_JOB_STATES


class StageStatus(StrEnum):
    """Where one stage of one job sits. Persisted as TEXT."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    SKIPPED = "skipped"
    FAILED = "failed"

    @property
    def is_done(self) -> bool:
        """True when a resume must NOT rerun this stage.

        SKIPPED means a fingerprint cache hit - the artifact already exists, so
        it is as done as SUCCEEDED. FAILED is deliberately not done: resuming a
        crashed batch must retry it.
        """
        return self in _DONE_STAGE_STATUSES


_TERMINAL_JOB_STATES = frozenset(
    {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED}
)
_DONE_STAGE_STATUSES = frozenset({StageStatus.SUCCEEDED, StageStatus.SKIPPED})
```

- [ ] **Step 4: Run to verify they pass**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/core -v
```

Expected: 15 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ytauto/core/models tests/unit/core
git commit -m "feat: add ArtifactRef and job lifecycle enums"
```

---

## Task 6: The `Stage` protocol

**Files:**
- Create: `src/ytauto/core/pipeline/stage.py`
- Test: `tests/unit/core/test_stage.py`

**Interfaces:**
- Consumes: `ArtifactRef`
- Produces:
  - `ProgressFn = Callable[[float, str], None]`
  - `JobContext(job_id: str, project_id: str, settings: Mapping[str, object], inputs: Mapping[str, tuple[ArtifactRef, ...]], workdir: Path)` — frozen
  - `JobContext.input(stage_id: str, name: str) -> ArtifactRef`
  - `StageResult(artifacts: tuple[ArtifactRef, ...], meta: Mapping[str, object])` — frozen
  - `StageResult.artifact(name: str) -> ArtifactRef`
  - `Stage` — `runtime_checkable` Protocol with `id`, `version`, `depends_on`, `fingerprint(ctx)`, `run(ctx, emit)`

`Path` in `JobContext.workdir` is deliberate and is the one place a path is allowed near a stage — but Task 7 forbids paths inside fingerprints, because they are machine-specific.

- [ ] **Step 1: Write the failing tests**

`tests/unit/core/test_stage.py`:

```python
from pathlib import Path

import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import hash_bytes
from ytauto.core.pipeline.stage import JobContext, Stage, StageResult


def _ref(name: str) -> ArtifactRef:
    return ArtifactRef(name=name, kind="blob", digest=hash_bytes(name.encode()))


def _ctx(**overrides: object) -> JobContext:
    base: dict[str, object] = {
        "job_id": "j1",
        "project_id": "p1",
        "settings": {"voice": "en-GB"},
        "inputs": {"ingest": (_ref("story"),)},
        "workdir": Path("/tmp/j1"),
    }
    base.update(overrides)
    return JobContext(**base)  # type: ignore[arg-type]


def test_context_exposes_a_named_input() -> None:
    assert _ctx().input("ingest", "story").name == "story"


def test_missing_input_stage_raises() -> None:
    with pytest.raises(ValidationError, match="rewrite"):
        _ctx().input("rewrite", "script")


def test_missing_input_name_raises() -> None:
    with pytest.raises(ValidationError, match="script"):
        _ctx().input("ingest", "script")


def test_context_is_frozen() -> None:
    with pytest.raises(AttributeError):
        _ctx().job_id = "other"  # type: ignore[misc]


def test_result_exposes_a_named_artifact() -> None:
    result = StageResult(artifacts=(_ref("narration"), _ref("timings")))
    assert result.artifact("timings").name == "timings"


def test_result_rejects_duplicate_artifact_names() -> None:
    with pytest.raises(ValidationError, match="duplicate"):
        StageResult(artifacts=(_ref("narration"), _ref("narration")))


def test_missing_result_artifact_raises() -> None:
    with pytest.raises(ValidationError, match="absent"):
        StageResult(artifacts=(_ref("narration"),)).artifact("absent")


def test_result_defaults_to_empty_meta() -> None:
    assert StageResult(artifacts=()).meta == {}


def test_a_conforming_class_satisfies_the_protocol() -> None:
    class Echo:
        id = "echo"
        version = 1
        depends_on: tuple[str, ...] = ()

        def fingerprint(self, ctx: JobContext) -> str:
            return "f" * 64

        def run(self, ctx: JobContext, emit: object) -> StageResult:
            return StageResult(artifacts=())

    assert isinstance(Echo(), Stage)


def test_a_class_missing_run_does_not_satisfy_the_protocol() -> None:
    class Partial:
        id = "partial"
        version = 1
        depends_on: tuple[str, ...] = ()

        def fingerprint(self, ctx: JobContext) -> str:
            return "f" * 64

    assert not isinstance(Partial(), Stage)
```

- [ ] **Step 2: Run to verify they fail**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/core/test_stage.py -v
```

Expected: FAIL — `No module named 'ytauto.core.pipeline.stage'`.

- [ ] **Step 3: Implement**

`src/ytauto/core/pipeline/stage.py`:

```python
"""The contract every pipeline stage implements."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable

from ytauto.core.errors import ValidationError
from ytauto.core.models.artifact import ArtifactRef

ProgressFn = Callable[[float, str], None]
"""Report progress as (fraction 0.0-1.0, human-readable message)."""


@dataclass(frozen=True)
class JobContext:
    """Everything a stage may see about the job it is running for.

    ``workdir`` is the one place a filesystem path legitimately reaches a
    stage. It must never reach a fingerprint - see core.pipeline.fingerprint.
    """

    job_id: str
    project_id: str
    settings: Mapping[str, object]
    inputs: Mapping[str, tuple[ArtifactRef, ...]]
    workdir: Path

    def input(self, stage_id: str, name: str) -> ArtifactRef:
        """Fetch one named artifact produced by an upstream stage.

        Raises:
            ValidationError: if the stage produced no artifacts for this job, or
                produced none by that name.
        """
        produced = self.inputs.get(stage_id)
        if produced is None:
            raise ValidationError(
                f"no inputs from stage {stage_id!r}; "
                f"available: {sorted(self.inputs)}"
            )
        for artifact in produced:
            if artifact.name == name:
                return artifact
        raise ValidationError(
            f"stage {stage_id!r} produced no artifact named {name!r}; "
            f"available: {sorted(a.name for a in produced)}"
        )


@dataclass(frozen=True)
class StageResult:
    """What a stage hands back: named artifacts plus optional metadata.

    Raises:
        ValidationError: if two artifacts share a name.
    """

    artifacts: tuple[ArtifactRef, ...]
    meta: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        names = [a.name for a in self.artifacts]
        if len(names) != len(set(names)):
            duplicates = sorted({n for n in names if names.count(n) > 1})
            raise ValidationError(f"duplicate artifact names: {duplicates}")

    def artifact(self, name: str) -> ArtifactRef:
        """Fetch one artifact by name.

        Raises:
            ValidationError: if no artifact has that name.
        """
        for artifact in self.artifacts:
            if artifact.name == name:
                return artifact
        raise ValidationError(
            f"no artifact named {name!r}; "
            f"produced: {sorted(a.name for a in self.artifacts)}"
        )


@runtime_checkable
class Stage(Protocol):
    """One node of the pipeline DAG.

    ``fingerprint`` must be a pure function of the context: same inputs and
    settings, same digest, across processes and interpreter restarts. The
    scheduler skips any stage whose fingerprint already has stored artifacts,
    so an unstable fingerprint silently disables all caching.
    """

    @property
    def id(self) -> str:
        """Stable identifier, unique within a pipeline."""

    @property
    def version(self) -> int:
        """Bump when the stage's behaviour changes, to invalidate old artifacts."""

    @property
    def depends_on(self) -> tuple[str, ...]:
        """IDs of stages whose artifacts this one consumes."""

    def fingerprint(self, ctx: JobContext) -> str:
        """Content hash of everything that determines this stage's output."""

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        """Do the work. Called only when the fingerprint missed."""
```

- [ ] **Step 4: Run to verify they pass**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/core/test_stage.py -v
```

Expected: 10 passed.

- [ ] **Step 5: Commit**

```bash
git add src/ytauto/core/pipeline/stage.py tests/unit/core/test_stage.py
git commit -m "feat: add Stage protocol with JobContext and StageResult"
```

---

## Task 7: Fingerprinting

**Files:**
- Create: `src/ytauto/core/pipeline/fingerprint.py`
- Test: `tests/unit/core/test_fingerprint.py`

**Interfaces:**
- Consumes: `ContentHash`, `ValidationError`
- Produces:
  - `FINGERPRINT_SCHEMA_VERSION: int`
  - `canonical_json(value: object) -> str`
  - `FingerprintSpec(stage_id, stage_version, provider_id, provider_version, input_digests: tuple[ContentHash, ...], settings: Mapping[str, object])` — frozen
  - `compute_fingerprint(spec: FingerprintSpec) -> str` — 64-char lowercase hex

**This is the highest-leverage module in the entire design.** A fingerprint that varies spuriously silently disables every cache benefit — crash-resume, cheap iteration, cross-project dedup — and nothing fails loudly. So the tests here are about *stability*, not just correctness.

Deliberate rules:
- **Paths are rejected.** They are machine-specific; including one makes every fingerprint local to one machine.
- **NaN and Infinity are rejected.** Not valid JSON and meaningless in a cache key.
- **Sets are sorted by their canonical encoding**, so element order never leaks in.
- **`1` and `1.0` fingerprint differently.** They are different settings.
- **The schema version is in the payload**, so a future change to canonicalisation invalidates cleanly instead of colliding.

- [ ] **Step 1: Write the failing tests**

`tests/unit/core/test_fingerprint.py`:

```python
import json
import os
import subprocess
import sys
from enum import StrEnum
from pathlib import Path

import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.models.content_hash import ContentHash
from ytauto.core.pipeline.fingerprint import (
    FINGERPRINT_SCHEMA_VERSION,
    FingerprintSpec,
    canonical_json,
    compute_fingerprint,
)

DIGEST_A = ContentHash("a" * 64)
DIGEST_B = ContentHash("b" * 64)


def _spec(**overrides: object) -> FingerprintSpec:
    base: dict[str, object] = {
        "stage_id": "rewrite",
        "stage_version": 1,
        "provider_id": "gemini-flash",
        "provider_version": "2026-05",
        "input_digests": (DIGEST_A,),
        "settings": {"tone": "dramatic", "max_words": 900},
    }
    base.update(overrides)
    return FingerprintSpec(**base)  # type: ignore[arg-type]


def test_produces_a_full_lowercase_hex_digest() -> None:
    value = compute_fingerprint(_spec())
    assert len(value) == 64
    assert value == value.lower()
    int(value, 16)


def test_is_deterministic() -> None:
    assert compute_fingerprint(_spec()) == compute_fingerprint(_spec())


def test_dict_insertion_order_does_not_matter() -> None:
    a = _spec(settings={"tone": "dramatic", "max_words": 900})
    b = _spec(settings={"max_words": 900, "tone": "dramatic"})
    assert compute_fingerprint(a) == compute_fingerprint(b)


def test_set_iteration_order_does_not_matter() -> None:
    a = _spec(settings={"langs": {"en", "fr", "de"}})
    b = _spec(settings={"langs": {"de", "en", "fr"}})
    assert compute_fingerprint(a) == compute_fingerprint(b)


def test_input_digest_order_does_matter() -> None:
    """Concatenation order changes the output, so it must change the key."""
    a = _spec(input_digests=(DIGEST_A, DIGEST_B))
    b = _spec(input_digests=(DIGEST_B, DIGEST_A))
    assert compute_fingerprint(a) != compute_fingerprint(b)


@pytest.mark.parametrize(
    "field",
    ["stage_id", "provider_id", "provider_version"],
)
def test_changing_any_identity_field_changes_the_hash(field: str) -> None:
    assert compute_fingerprint(_spec()) != compute_fingerprint(_spec(**{field: "other"}))


def test_bumping_stage_version_changes_the_hash() -> None:
    assert compute_fingerprint(_spec()) != compute_fingerprint(_spec(stage_version=2))


def test_changing_a_setting_changes_the_hash() -> None:
    assert compute_fingerprint(_spec()) != compute_fingerprint(
        _spec(settings={"tone": "calm", "max_words": 900})
    )


def test_int_and_float_are_distinct_settings() -> None:
    assert compute_fingerprint(_spec(settings={"gain": 1})) != compute_fingerprint(
        _spec(settings={"gain": 1.0})
    )


def test_schema_version_is_part_of_the_payload() -> None:
    """A future canonicalisation change must invalidate, not collide."""
    payload = json.loads(canonical_json({"schema": FINGERPRINT_SCHEMA_VERSION}))
    assert payload["schema"] == FINGERPRINT_SCHEMA_VERSION


def test_enums_encode_as_their_value() -> None:
    class Tone(StrEnum):
        DRAMATIC = "dramatic"

    assert compute_fingerprint(_spec(settings={"tone": Tone.DRAMATIC})) == (
        compute_fingerprint(_spec(settings={"tone": "dramatic"}))
    )


def test_paths_are_rejected() -> None:
    """A path is machine-specific; including one makes the cache non-portable."""
    with pytest.raises(ValidationError, match="path"):
        compute_fingerprint(_spec(settings={"workdir": Path("/tmp/x")}))


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_floats_are_rejected(bad: float) -> None:
    with pytest.raises(ValidationError):
        compute_fingerprint(_spec(settings={"gain": bad}))


def test_unsupported_types_are_rejected() -> None:
    with pytest.raises(ValidationError, match="object"):
        compute_fingerprint(_spec(settings={"handle": object()}))


def test_nested_structures_are_canonicalised() -> None:
    a = _spec(settings={"outer": {"b": [1, {"y": 2, "x": 1}], "a": 0}})
    b = _spec(settings={"outer": {"a": 0, "b": [1, {"x": 1, "y": 2}]}})
    assert compute_fingerprint(a) == compute_fingerprint(b)


def test_list_order_does_matter() -> None:
    a = _spec(settings={"beats": ["hook", "body"]})
    b = _spec(settings={"beats": ["body", "hook"]})
    assert compute_fingerprint(a) != compute_fingerprint(b)


def test_is_stable_across_interpreter_restarts() -> None:
    """The test that matters most: no PYTHONHASHSEED dependence, no id(), no
    insertion-order reliance. A fingerprint that varies per process silently
    disables every cache benefit and fails nothing loudly."""
    script = (
        "from ytauto.core.pipeline.fingerprint import FingerprintSpec, compute_fingerprint\n"
        "from ytauto.core.models.content_hash import ContentHash\n"
        "print(compute_fingerprint(FingerprintSpec(\n"
        "    stage_id='rewrite', stage_version=1, provider_id='gemini-flash',\n"
        "    provider_version='2026-05', input_digests=(ContentHash('a'*64),),\n"
        "    settings={'tone': 'dramatic', 'max_words': 900, 'langs': {'en','fr','de'}},\n"
        ")))\n"
    )
    seen = set()
    for seed in ("0", "1", "12345"):
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            # Inherit the environment and override only the seed. A minimal env
            # is not an option on Windows: without SystemRoot the interpreter
            # fails to initialize, and the test would fail for a reason that has
            # nothing to do with fingerprint stability.
            env={**os.environ, "PYTHONHASHSEED": seed},
            check=True,
        )
        seen.add(result.stdout.strip())

    expected = compute_fingerprint(
        _spec(settings={"tone": "dramatic", "max_words": 900, "langs": {"en", "fr", "de"}})
    )
    assert seen == {expected}, f"fingerprint varied across processes: {seen}"
```

Note the subprocess `env` deliberately omits everything but `PYTHONHASHSEED` and an empty `PATH`; the package is installed in the venv running the test, so `sys.executable` resolves it.

- [ ] **Step 2: Run to verify they fail**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/core/test_fingerprint.py -v
```

Expected: FAIL — `No module named 'ytauto.core.pipeline.fingerprint'`.

- [ ] **Step 3: Implement**

`src/ytauto/core/pipeline/fingerprint.py`:

```python
"""Content-addressed stage fingerprinting.

A stage whose fingerprint already has stored artifacts is skipped. One
mechanism therefore delivers crash-resume, cheap iteration, and cross-project
dedup - and an unstable fingerprint silently disables all three while failing
nothing. Canonicalisation here is deliberately strict for that reason.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import PurePath
from typing import Any

from ytauto.core.errors import ValidationError
from ytauto.core.models.content_hash import ContentHash

FINGERPRINT_SCHEMA_VERSION = 1
"""Bump when canonicalisation changes, so old artifacts invalidate rather than
colliding with differently-computed new ones."""


def _encode(obj: object) -> Any:
    """Convert a value json.dumps cannot handle into one it can.

    Raises:
        ValidationError: for paths, non-finite floats, and unsupported types.
    """
    if isinstance(obj, PurePath):
        raise ValidationError(
            f"a path may not appear in a fingerprint: {obj!r} is machine-specific"
        )
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, (set, frozenset)):
        return sorted(obj, key=canonical_json)
    if isinstance(obj, bytes):
        return obj.hex()
    raise ValidationError(
        f"cannot fingerprint a value of type {type(obj).__name__}: {obj!r}"
    )


def canonical_json(value: object) -> str:
    """Encode a value as deterministic JSON: sorted keys, no whitespace.

    ``allow_nan=False`` is what rejects NaN and Infinity: they are not valid
    JSON and are meaningless as a cache key. It is load-bearing, not decorative
    - without it json.dumps happily emits bare ``NaN``, which no other JSON
    reader would accept and which never compares equal to itself.

    Raises:
        ValidationError: if the value contains a path, a non-finite float, a
            circular reference, or an unsupported type.
    """
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
            default=_encode,
        )
    except ValueError as exc:  # non-finite float, or a circular reference
        raise ValidationError(f"value is not canonicalisable: {exc}") from exc


@dataclass(frozen=True)
class FingerprintSpec:
    """Everything that determines a stage's output.

    ``input_digests`` is ordered because order changes the result - concatenating
    two clips the other way round produces a different video.
    """

    stage_id: str
    stage_version: int
    provider_id: str
    provider_version: str
    input_digests: tuple[ContentHash, ...]
    settings: Mapping[str, object]


def compute_fingerprint(spec: FingerprintSpec) -> str:
    """Return the 64-char lowercase hex fingerprint for a stage execution.

    Raises:
        ValidationError: if ``settings`` contains a path, a non-finite float, or
            an unsupported type.
    """
    payload = {
        "schema": FINGERPRINT_SCHEMA_VERSION,
        "stage_id": spec.stage_id,
        "stage_version": spec.stage_version,
        "provider_id": spec.provider_id,
        "provider_version": spec.provider_version,
        "input_digests": list(spec.input_digests),
        "settings": dict(spec.settings),
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
```

- [ ] **Step 4: Run to verify they pass**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/core/test_fingerprint.py -v
```

Expected: 20 passed.

- [ ] **Step 5: Prove the stability test is load-bearing**

Temporarily change `sort_keys=True` to `sort_keys=False` in `canonical_json`. Confirm `test_dict_insertion_order_does_not_matter` FAILS. Then restore it and instead change the set branch of `_encode` to `list(obj)` and confirm `test_set_iteration_order_does_not_matter` and `test_is_stable_across_interpreter_restarts` FAIL. Restore, confirm all PASS. Paste all outputs — this module's correctness rests entirely on these guards.

- [ ] **Step 6: Commit**

```bash
git add src/ytauto/core/pipeline/fingerprint.py tests/unit/core/test_fingerprint.py
git commit -m "feat: add stable stage fingerprinting with strict canonicalisation"
```

---

## Task 8: The pipeline DAG

**Files:**
- Create: `src/ytauto/core/pipeline/graph.py`
- Test: `tests/unit/core/test_graph.py`

**Interfaces:**
- Consumes: `Stage`, `ValidationError`
- Produces:
  - `Pipeline(id: str, stages: tuple[Stage, ...])` — frozen, validating on construction
  - `Pipeline.stage_by_id(stage_id: str) -> Stage`
  - `Pipeline.topological_order() -> tuple[Stage, ...]`
  - `Pipeline.downstream_of(stage_id: str) -> frozenset[str]`

`downstream_of` is what turns "the user edited the script" into "rerun stages 3–9, reuse 1–2". Ordering must be **deterministic** — two runs of the same pipeline must produce the same order, or fingerprints computed from upstream artifacts could vary.

- [ ] **Step 1: Write the failing tests**

`tests/unit/core/test_graph.py`:

```python
import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.pipeline.graph import Pipeline
from ytauto.core.pipeline.stage import JobContext, StageResult


class FakeStage:
    """Minimal Stage implementation for graph tests."""

    def __init__(self, stage_id: str, depends_on: tuple[str, ...] = ()) -> None:
        self.id = stage_id
        self.version = 1
        self.depends_on = depends_on

    def fingerprint(self, ctx: JobContext) -> str:
        return "f" * 64

    def run(self, ctx: JobContext, emit: object) -> StageResult:
        return StageResult(artifacts=())


def _linear() -> Pipeline:
    return Pipeline(
        id="shorts",
        stages=(
            FakeStage("ingest"),
            FakeStage("rewrite", ("ingest",)),
            FakeStage("tts", ("rewrite",)),
        ),
    )


def test_topological_order_respects_dependencies() -> None:
    assert [s.id for s in _linear().topological_order()] == ["ingest", "rewrite", "tts"]


def test_topological_order_is_deterministic_regardless_of_declaration_order() -> None:
    """Two runs must plan identically; a varying order could vary fingerprints."""
    shuffled = Pipeline(
        id="shorts",
        stages=(
            FakeStage("tts", ("rewrite",)),
            FakeStage("ingest"),
            FakeStage("rewrite", ("ingest",)),
        ),
    )
    assert [s.id for s in shuffled.topological_order()] == ["ingest", "rewrite", "tts"]


def test_independent_stages_are_ordered_by_id() -> None:
    pipeline = Pipeline(
        id="p",
        stages=(FakeStage("zebra"), FakeStage("apple"), FakeStage("mango")),
    )
    assert [s.id for s in pipeline.topological_order()] == ["apple", "mango", "zebra"]


def test_diamond_dependencies_resolve() -> None:
    pipeline = Pipeline(
        id="p",
        stages=(
            FakeStage("root"),
            FakeStage("left", ("root",)),
            FakeStage("right", ("root",)),
            FakeStage("join", ("left", "right")),
        ),
    )
    order = [s.id for s in pipeline.topological_order()]
    assert order[0] == "root"
    assert order[-1] == "join"
    assert set(order[1:3]) == {"left", "right"}


def test_a_cycle_is_rejected() -> None:
    with pytest.raises(ValidationError, match="cycle"):
        Pipeline(id="p", stages=(FakeStage("a", ("b",)), FakeStage("b", ("a",))))


def test_a_self_dependency_is_rejected() -> None:
    with pytest.raises(ValidationError, match="cycle"):
        Pipeline(id="p", stages=(FakeStage("a", ("a",)),))


def test_an_unknown_dependency_is_rejected() -> None:
    with pytest.raises(ValidationError, match="ghost"):
        Pipeline(id="p", stages=(FakeStage("a", ("ghost",)),))


def test_duplicate_stage_ids_are_rejected() -> None:
    with pytest.raises(ValidationError, match="duplicate"):
        Pipeline(id="p", stages=(FakeStage("a"), FakeStage("a")))


def test_an_empty_pipeline_is_rejected() -> None:
    with pytest.raises(ValidationError, match="empty"):
        Pipeline(id="p", stages=())


def test_stage_by_id_returns_the_stage() -> None:
    assert _linear().stage_by_id("rewrite").id == "rewrite"


def test_stage_by_id_rejects_an_unknown_id() -> None:
    with pytest.raises(ValidationError, match="nope"):
        _linear().stage_by_id("nope")


def test_downstream_is_transitive() -> None:
    """Editing the script must invalidate tts too, not just rewrite."""
    assert _linear().downstream_of("ingest") == {"rewrite", "tts"}


def test_downstream_of_a_leaf_is_empty() -> None:
    assert _linear().downstream_of("tts") == frozenset()


def test_downstream_excludes_the_stage_itself() -> None:
    assert "rewrite" not in _linear().downstream_of("rewrite")


def test_downstream_across_a_diamond_reaches_the_join() -> None:
    pipeline = Pipeline(
        id="p",
        stages=(
            FakeStage("root"),
            FakeStage("left", ("root",)),
            FakeStage("right", ("root",)),
            FakeStage("join", ("left", "right")),
        ),
    )
    assert pipeline.downstream_of("left") == {"join"}
    assert pipeline.downstream_of("root") == {"left", "right", "join"}


def test_downstream_rejects_an_unknown_stage() -> None:
    with pytest.raises(ValidationError, match="nope"):
        _linear().downstream_of("nope")
```

- [ ] **Step 2: Run to verify they fail**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/core/test_graph.py -v
```

Expected: FAIL — `No module named 'ytauto.core.pipeline.graph'`.

- [ ] **Step 3: Implement**

`src/ytauto/core/pipeline/graph.py`:

```python
"""The stage DAG: validation, deterministic ordering, and invalidation."""

from __future__ import annotations

from dataclasses import dataclass, field

from ytauto.core.errors import ValidationError
from ytauto.core.pipeline.stage import Stage


@dataclass(frozen=True)
class Pipeline:
    """A validated directed acyclic graph of stages.

    Validation happens once, at construction, so nothing downstream has to
    re-check for cycles or dangling dependencies.

    Raises:
        ValidationError: if the pipeline is empty, has duplicate stage IDs,
            references an unknown dependency, or contains a cycle.
    """

    id: str
    stages: tuple[Stage, ...]
    _by_id: dict[str, Stage] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not self.stages:
            raise ValidationError(f"pipeline {self.id!r} is empty")

        ids = [stage.id for stage in self.stages]
        if len(ids) != len(set(ids)):
            duplicates = sorted({i for i in ids if ids.count(i) > 1})
            raise ValidationError(f"duplicate stage ids: {duplicates}")

        known = set(ids)
        for stage in self.stages:
            unknown = sorted(set(stage.depends_on) - known)
            if unknown:
                raise ValidationError(
                    f"stage {stage.id!r} depends on unknown stage(s): {unknown}"
                )

        object.__setattr__(self, "_by_id", {stage.id: stage for stage in self.stages})
        self._assert_acyclic()

    def _assert_acyclic(self) -> None:
        """Detect cycles via depth-first search with a recursion stack."""
        visiting: set[str] = set()
        done: set[str] = set()

        def visit(stage_id: str, trail: tuple[str, ...]) -> None:
            if stage_id in done:
                return
            if stage_id in visiting:
                cycle = " -> ".join((*trail, stage_id))
                raise ValidationError(f"pipeline {self.id!r} contains a cycle: {cycle}")
            visiting.add(stage_id)
            for dependency in sorted(self._by_id[stage_id].depends_on):
                visit(dependency, (*trail, stage_id))
            visiting.discard(stage_id)
            done.add(stage_id)

        for stage in self.stages:
            visit(stage.id, ())

    def stage_by_id(self, stage_id: str) -> Stage:
        """Look up one stage.

        Raises:
            ValidationError: if no stage has that ID.
        """
        stage = self._by_id.get(stage_id)
        if stage is None:
            raise ValidationError(
                f"no stage {stage_id!r} in pipeline {self.id!r}; "
                f"known: {sorted(self._by_id)}"
            )
        return stage

    def topological_order(self) -> tuple[Stage, ...]:
        """Dependencies first; ties broken by stage ID.

        The tiebreak makes ordering independent of declaration order, so two
        runs of the same pipeline plan identically. A varying order could vary
        the artifacts fed into a downstream fingerprint.
        """
        ordered: list[Stage] = []
        placed: set[str] = set()

        def place(stage_id: str) -> None:
            if stage_id in placed:
                return
            for dependency in sorted(self._by_id[stage_id].depends_on):
                place(dependency)
            placed.add(stage_id)
            ordered.append(self._by_id[stage_id])

        for stage_id in sorted(self._by_id):
            place(stage_id)
        return tuple(ordered)

    def downstream_of(self, stage_id: str) -> frozenset[str]:
        """Every stage that transitively depends on this one, excluding itself.

        This is what turns "the script changed" into "rerun these stages and
        reuse the rest".

        Raises:
            ValidationError: if no stage has that ID.
        """
        self.stage_by_id(stage_id)
        affected: set[str] = set()
        frontier = {stage_id}
        while frontier:
            nxt = {
                stage.id
                for stage in self.stages
                if set(stage.depends_on) & frontier and stage.id not in affected
            }
            affected |= nxt
            frontier = nxt
        affected.discard(stage_id)
        return frozenset(affected)
```

- [ ] **Step 4: Run to verify they pass**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/core/test_graph.py -v
```

Expected: 16 passed.

- [ ] **Step 5: Prove the determinism guard is load-bearing**

Temporarily change `for stage_id in sorted(self._by_id):` to `for stage_id in self._by_id:` in `topological_order`. Confirm `test_topological_order_is_deterministic_regardless_of_declaration_order` and `test_independent_stages_are_ordered_by_id` FAIL. Restore, confirm PASS. Paste both outputs.

- [ ] **Step 6: Commit**

```bash
git add src/ytauto/core/pipeline/graph.py tests/unit/core/test_graph.py
git commit -m "feat: add validated pipeline DAG with deterministic ordering"
```

---

## Task 9: Provider ports and capability descriptors

**Files:**
- Create: `src/ytauto/core/ports/capability.py`
- Create: `src/ytauto/core/ports/providers.py`
- Test: `tests/unit/core/test_ports.py`

**Interfaces:**
- Consumes: nothing beyond stdlib.
- Produces:
  - `CostModel` StrEnum: `FREE`, `PER_TOKEN`, `PER_CHAR`, `PER_SECOND`, `PER_IMAGE`
  - `LatencyClass` StrEnum: `INSTANT`, `FAST`, `SLOW`
  - `CapabilityDescriptor(provider_id, version, cost_model, latency_class, offline, requires_gpu, vram_mb, quality_tier, languages)` — frozen, validating
  - Protocols in `providers.py`: `StorySource`, `ScriptGenerator`, `SpeechSynthesizer`, `Transcriber`, `VisualStrategy`, `ImageGenerator`, `ThumbnailRenderer`, `Publisher`

**Why now, with no providers to implement them.** Phase 2 builds the first real providers, and these Protocols are what it builds against. Defining them one phase ahead is the difference between Phase 2 implementing a contract and Phase 2 inventing one per provider. All eight are placed in a single `providers.py` because they are small, change together, and are read together — splitting eight ~10-line Protocols across eight files would be structure for its own sake.

`Publisher` is defined and deliberately unimplemented: the YouTube Data API bills an upload at 1,600 quota units against a 10,000/day default, capping ~6 uploads/day regardless of render throughput. The seam exists so it can be added without structural change.

- [ ] **Step 1: Write the failing tests**

`tests/unit/core/test_ports.py`:

```python
import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.ports.capability import CapabilityDescriptor, CostModel, LatencyClass
from ytauto.core.ports.providers import (
    ImageGenerator,
    Publisher,
    ScriptGenerator,
    SpeechSynthesizer,
    StorySource,
    ThumbnailRenderer,
    Transcriber,
    VisualStrategy,
)


def _descriptor(**overrides: object) -> CapabilityDescriptor:
    base: dict[str, object] = {
        "provider_id": "edge-tts",
        "version": "7.0",
        "cost_model": CostModel.FREE,
        "latency_class": LatencyClass.FAST,
        "offline": False,
        "requires_gpu": False,
        "vram_mb": None,
        "quality_tier": 4,
        "languages": frozenset({"en", "fr"}),
    }
    base.update(overrides)
    return CapabilityDescriptor(**base)  # type: ignore[arg-type]


def test_descriptor_is_frozen() -> None:
    with pytest.raises(AttributeError):
        _descriptor().provider_id = "other"  # type: ignore[misc]


@pytest.mark.parametrize("tier", [0, 6, -1])
def test_quality_tier_must_be_one_to_five(tier: int) -> None:
    with pytest.raises(ValidationError, match="quality_tier"):
        _descriptor(quality_tier=tier)


def test_a_gpu_provider_must_declare_its_vram() -> None:
    """The governor sizes GPU leases from this; None would mean 'unbounded'
    on a 4 GB card."""
    with pytest.raises(ValidationError, match="vram_mb"):
        _descriptor(requires_gpu=True, vram_mb=None)


def test_a_gpu_provider_with_vram_is_accepted() -> None:
    assert _descriptor(requires_gpu=True, vram_mb=2048).vram_mb == 2048


def test_a_non_gpu_provider_may_not_claim_vram() -> None:
    with pytest.raises(ValidationError, match="vram_mb"):
        _descriptor(requires_gpu=False, vram_mb=2048)


def test_free_providers_are_identified() -> None:
    assert _descriptor(cost_model=CostModel.FREE).is_free
    assert not _descriptor(cost_model=CostModel.PER_TOKEN).is_free


def test_empty_provider_id_is_rejected() -> None:
    with pytest.raises(ValidationError, match="provider_id"):
        _descriptor(provider_id="")


@pytest.mark.parametrize(
    "port",
    [
        StorySource,
        ScriptGenerator,
        SpeechSynthesizer,
        Transcriber,
        VisualStrategy,
        ImageGenerator,
        ThumbnailRenderer,
        Publisher,
    ],
)
def test_every_port_requires_a_capability_descriptor(port: type) -> None:
    """Provider selection reads `capabilities` on every port uniformly."""
    assert "capabilities" in port.__annotations__ or hasattr(port, "capabilities")


def test_a_conforming_synthesizer_satisfies_the_protocol() -> None:
    class Fake:
        capabilities = _descriptor()

        def synthesize(self, text: str, *, voice: str) -> bytes:
            return b""

    assert isinstance(Fake(), SpeechSynthesizer)


def test_a_synthesizer_missing_synthesize_does_not_satisfy_it() -> None:
    class Fake:
        capabilities = _descriptor()

    assert not isinstance(Fake(), SpeechSynthesizer)
```

- [ ] **Step 2: Run to verify they fail**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/core/test_ports.py -v
```

Expected: FAIL — `No module named 'ytauto.core.ports.capability'`.

- [ ] **Step 3: Implement `capability.py`**

```python
"""Declarative capability metadata every provider ships.

This is what makes "keep operating costs extremely low" a system property
rather than an intention: a cost policy can prefer free and offline providers
and escalate only on explicit opt-in, because every provider states its terms
in the same shape.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from ytauto.core.errors import ValidationError


class CostModel(StrEnum):
    FREE = "free"
    PER_TOKEN = "per_token"
    PER_CHAR = "per_char"
    PER_SECOND = "per_second"
    PER_IMAGE = "per_image"


class LatencyClass(StrEnum):
    INSTANT = "instant"
    FAST = "fast"
    SLOW = "slow"


@dataclass(frozen=True)
class CapabilityDescriptor:
    """What a provider costs, needs, and is good for.

    Raises:
        ValidationError: if ``provider_id`` is empty, ``quality_tier`` is
            outside 1-5, or ``requires_gpu`` and ``vram_mb`` disagree.
    """

    provider_id: str
    version: str
    cost_model: CostModel
    latency_class: LatencyClass
    offline: bool
    requires_gpu: bool
    vram_mb: int | None
    quality_tier: int
    languages: frozenset[str]

    def __post_init__(self) -> None:
        if not self.provider_id:
            raise ValidationError("provider_id must not be empty")
        if not 1 <= self.quality_tier <= 5:
            raise ValidationError(
                f"quality_tier must be 1-5, got {self.quality_tier}"
            )
        if self.requires_gpu and self.vram_mb is None:
            raise ValidationError(
                f"{self.provider_id} requires a GPU but declares no vram_mb; "
                "the resource governor cannot schedule it safely"
            )
        if not self.requires_gpu and self.vram_mb is not None:
            raise ValidationError(
                f"{self.provider_id} declares vram_mb but not requires_gpu"
            )

    @property
    def is_free(self) -> bool:
        """True when using this provider costs nothing per call."""
        return self.cost_model is CostModel.FREE
```

- [ ] **Step 4: Implement `providers.py`**

```python
"""The plugin seams.

Eight Protocols, one per provider family. A new TTS engine or LLM is added by
implementing the relevant Protocol and registering an entry point - with no
change to core/ or app/.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ytauto.core.ports.capability import CapabilityDescriptor


@runtime_checkable
class StorySource(Protocol):
    """Fetches or imports raw stories."""

    capabilities: CapabilityDescriptor

    def fetch(self, reference: str) -> str:
        """Return raw story text for a URL, file path, or identifier."""


@runtime_checkable
class ScriptGenerator(Protocol):
    """Rewrites a raw story into a narration script."""

    capabilities: CapabilityDescriptor

    def rewrite(self, story: str, *, style: str) -> str:
        """Return the rewritten script."""


@runtime_checkable
class SpeechSynthesizer(Protocol):
    """Turns script text into narration audio."""

    capabilities: CapabilityDescriptor

    def synthesize(self, text: str, *, voice: str) -> bytes:
        """Return encoded audio bytes."""


@runtime_checkable
class Transcriber(Protocol):
    """Produces word-level timings for narration audio.

    Two implementations exist by design: one consuming TTS word-boundary
    metadata (free, instant, no GPU) and one running ASR (universal, needs a
    GPU lease). Same port, very different cost.
    """

    capabilities: CapabilityDescriptor

    def transcribe(self, audio: bytes) -> tuple[tuple[str, float, float], ...]:
        """Return (word, start_seconds, end_seconds) triples."""


@runtime_checkable
class VisualStrategy(Protocol):
    """Populates a timeline's visual segments."""

    capabilities: CapabilityDescriptor

    def plan(self, duration_s: float, *, seed: int) -> tuple[str, ...]:
        """Return ordered visual asset references covering the duration."""


@runtime_checkable
class ImageGenerator(Protocol):
    """Generates a still image from a prompt."""

    capabilities: CapabilityDescriptor

    def generate(self, prompt: str, *, width: int, height: int) -> bytes:
        """Return encoded image bytes."""


@runtime_checkable
class ThumbnailRenderer(Protocol):
    """Composes a video thumbnail."""

    capabilities: CapabilityDescriptor

    def render(self, title: str, *, background: bytes) -> bytes:
        """Return encoded thumbnail image bytes."""


@runtime_checkable
class Publisher(Protocol):
    """Reserved seam - no implementation ships.

    The YouTube Data API bills an upload at 1,600 quota units against a
    10,000/day default, capping roughly 6 uploads/day regardless of how many
    videos are rendered. Export-to-file is the supported path; this exists so
    publishing can be added later without structural change.
    """

    capabilities: CapabilityDescriptor

    def publish(self, video_path: str, *, title: str, description: str) -> str:
        """Return the published video's identifier."""
```

- [ ] **Step 5: Run to verify they pass**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/core/test_ports.py -v
```

Expected: 17 passed.

- [ ] **Step 6: Commit**

```bash
git add src/ytauto/core/ports tests/unit/core/test_ports.py
git commit -m "feat: add provider ports and capability descriptors"
```

---

## Task 10: `ArtifactStore` — the fingerprint cache

**Files:**
- Create: `src/ytauto/infra/artifacts.py`
- Test: `tests/unit/infra/test_artifacts.py`

**Interfaces:**
- Consumes: `CasStore`, `ArtifactRef`, `transaction`, `utc_now_iso`, `ValidationError`
- Produces:
  - `ArtifactStore(cas: CasStore, conn: sqlite3.Connection)`
  - `lookup(fingerprint: str) -> tuple[ArtifactRef, ...] | None`
  - `record(fingerprint: str, stage_id: str, artifacts: Sequence[ArtifactRef]) -> bool`
  - `forget(fingerprint: str) -> None`

**This is where the cache actually happens.** `lookup` returning non-`None` is what lets the scheduler skip a stage.

Three correctness details:

- **`record` retains each digest exactly once**, so the evictor cannot delete a cached stage output. It returns `True` on a first write and `False` when the fingerprint was already recorded — and on `False` it must **not** retain again, or refcounts inflate on every resume and nothing is ever evictable.
- **`forget` releases** what `record` retained, keeping refcounts symmetric.
- **`lookup` verifies the blobs still exist and self-heals when they don't.**
  This closes a real window: `record` commits its rows inside a transaction but
  retains blobs *after* it, because `CasStore.retain()` opens its own
  transaction and `transaction()` is not re-entrant (carry-forward §1.2, whose
  savepoint fix lands in Phase 1b). A crash in that window leaves rows with
  refcount 0, the evictor is then free to delete the blobs, and a later
  `lookup` would report a cache hit for artifacts that no longer exist — the
  scheduler would skip a stage whose output is gone. Treating a missing blob as
  a miss, and dropping the stale rows, makes the cache safe against that window
  and against any other cause of blob loss.

- [ ] **Step 1: Write the failing tests**

`tests/unit/infra/test_artifacts.py`:

```python
import sqlite3

import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.cas.store import CasStore

FP = "f" * 64


@pytest.fixture()
def artifacts(store: CasStore, db_conn: sqlite3.Connection) -> ArtifactStore:
    """Both fixtures come from tests/unit/infra/conftest.py and share a connection."""
    return ArtifactStore(cas=store, conn=db_conn)


def _put(store: CasStore, name: str, data: bytes) -> ArtifactRef:
    return ArtifactRef(name=name, kind="blob", digest=store.put_bytes(data, kind="blob"))


def test_lookup_misses_on_an_unknown_fingerprint(artifacts: ArtifactStore) -> None:
    assert artifacts.lookup(FP) is None


def test_record_then_lookup_round_trips(artifacts: ArtifactStore, store: CasStore) -> None:
    ref = _put(store, "narration", b"audio")
    assert artifacts.record(FP, "tts", [ref]) is True
    assert artifacts.lookup(FP) == (ref,)


def test_lookup_returns_several_artifacts_in_name_order(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    timings = _put(store, "timings", b"json")
    narration = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [timings, narration])
    assert [a.name for a in artifacts.lookup(FP) or ()] == ["narration", "timings"]


def test_record_retains_each_digest_once(artifacts: ArtifactStore, store: CasStore) -> None:
    """Retaining is what stops the evictor deleting a cached stage output."""
    ref = _put(store, "narration", b"audio")
    assert store.refcount(ref.digest) == 0
    artifacts.record(FP, "tts", [ref])
    assert store.refcount(ref.digest) == 1


def test_recording_the_same_fingerprint_twice_does_not_inflate_refcounts(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    """A resume re-records the same fingerprint. Double-retaining would make
    the artifact permanently unevictable."""
    ref = _put(store, "narration", b"audio")
    assert artifacts.record(FP, "tts", [ref]) is True
    assert artifacts.record(FP, "tts", [ref]) is False
    assert store.refcount(ref.digest) == 1


def test_forget_releases_and_removes(artifacts: ArtifactStore, store: CasStore) -> None:
    ref = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [ref])
    artifacts.forget(FP)
    assert artifacts.lookup(FP) is None
    assert store.refcount(ref.digest) == 0


def test_forget_is_idempotent(artifacts: ArtifactStore, store: CasStore) -> None:
    ref = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [ref])
    artifacts.forget(FP)
    artifacts.forget(FP)
    assert store.refcount(ref.digest) == 0


def test_recording_no_artifacts_is_rejected(artifacts: ArtifactStore) -> None:
    with pytest.raises(ValidationError, match="no artifacts"):
        artifacts.record(FP, "tts", [])


def test_a_malformed_fingerprint_is_rejected(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    with pytest.raises(ValidationError, match="fingerprint"):
        artifacts.record("not-a-fingerprint", "tts", [_put(store, "n", b"x")])


def test_lookup_rejects_a_malformed_fingerprint(artifacts: ArtifactStore) -> None:
    with pytest.raises(ValidationError, match="fingerprint"):
        artifacts.lookup("nope")


def test_a_failed_record_leaves_no_partial_state(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    """The row write and the retain must land together or not at all."""
    good = _put(store, "narration", b"audio")
    missing = ArtifactRef(name="ghost", kind="blob", digest="c" * 64)  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        artifacts.record(FP, "tts", [good, missing])
    assert artifacts.lookup(FP) is None
    assert store.refcount(good.digest) == 0


def test_lookup_treats_a_vanished_blob_as_a_miss(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    """record() commits rows before retaining blobs, so a crash in that window
    leaves rows pointing at evictable blobs. Reporting a hit for artifacts that
    no longer exist would make the scheduler skip a stage whose output is gone."""
    ref = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [ref])
    store.path_for(ref.digest).unlink()

    assert artifacts.lookup(FP) is None


def test_lookup_drops_the_stale_rows_it_finds(
    artifacts: ArtifactStore, store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """Self-healing: a miss caused by a vanished blob must not be re-detected
    on every subsequent lookup."""
    ref = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [ref])
    store.path_for(ref.digest).unlink()

    artifacts.lookup(FP)

    remaining = db_conn.execute(
        "SELECT count(*) FROM artifacts WHERE fingerprint = ?", (FP,)
    ).fetchone()[0]
    assert remaining == 0


def test_a_partially_vanished_set_is_a_miss(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    """One missing artifact invalidates the whole stage output, not just itself."""
    narration = _put(store, "narration", b"audio")
    timings = _put(store, "timings", b"json")
    artifacts.record(FP, "tts", [narration, timings])
    store.path_for(timings.digest).unlink()

    assert artifacts.lookup(FP) is None
```

- [ ] **Step 2: Run to verify they fail**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_artifacts.py -v
```

Expected: FAIL — `No module named 'ytauto.infra.artifacts'`.

- [ ] **Step 3: Implement**

`src/ytauto/infra/artifacts.py`:

```python
"""Maps stage fingerprints to the artifacts they produced.

A fingerprint with stored artifacts means the stage can be skipped. That single
lookup is what delivers crash-resume, cheap iteration, and cross-project dedup.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence

from ytauto.core.errors import ValidationError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash, validate_digest
from ytauto.infra.cas.store import CasStore
from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import transaction


class ArtifactStore:
    """Fingerprint-keyed index over the content-addressed store."""

    def __init__(self, cas: CasStore, conn: sqlite3.Connection) -> None:
        self._cas = cas
        self._conn = conn

    @staticmethod
    def _validate_fingerprint(fingerprint: str) -> str:
        """Raises:
        ValidationError: if the fingerprint is not a sha256 hex digest.
        """
        try:
            validate_digest(fingerprint)
        except ValidationError as exc:
            raise ValidationError(f"not a valid fingerprint: {fingerprint!r}") from exc
        return fingerprint

    def lookup(self, fingerprint: str) -> tuple[ArtifactRef, ...] | None:
        """Return the artifacts recorded for this fingerprint, or None on a miss.

        A recorded fingerprint whose blobs are no longer in the store counts as
        a miss, and its stale rows are dropped. This is what makes the cache
        safe: ``record`` commits rows before it retains blobs (see the class
        docstring), so a crash in that window can leave rows pointing at
        evictable blobs. Reporting a hit for artifacts that no longer exist
        would make the scheduler skip a stage whose output is gone.

        Raises:
            ValidationError: if ``fingerprint`` is malformed.
        """
        self._validate_fingerprint(fingerprint)
        rows = self._conn.execute(
            "SELECT name, kind, digest FROM artifacts WHERE fingerprint = ? "
            "ORDER BY name ASC",
            (fingerprint,),
        ).fetchall()
        if not rows:
            return None

        found = tuple(
            ArtifactRef(
                name=row["name"], kind=row["kind"], digest=ContentHash(row["digest"])
            )
            for row in rows
        )
        if all(self._cas.exists(artifact.digest) for artifact in found):
            return found

        self._drop_rows(fingerprint)
        return None

    def _drop_rows(self, fingerprint: str) -> None:
        """Delete a fingerprint's rows without releasing blobs.

        Used when the blobs are already gone, so releasing would drive
        refcounts below what the remaining holders expect.
        """
        with transaction(self._conn, immediate=True):
            self._conn.execute(
                "DELETE FROM artifacts WHERE fingerprint = ?", (fingerprint,)
            )

    def record(
        self, fingerprint: str, stage_id: str, artifacts: Sequence[ArtifactRef]
    ) -> bool:
        """Store the artifacts for a fingerprint and retain their blobs.

        Returns True on a first write, False if this fingerprint was already
        recorded. On False nothing is retained again - double-retaining on every
        resume would inflate refcounts and make the artifact permanently
        unevictable.

        Raises:
            ValidationError: if ``fingerprint`` is malformed, ``artifacts`` is
                empty, or a referenced blob is absent from the CAS.
        """
        self._validate_fingerprint(fingerprint)
        if not artifacts:
            raise ValidationError(f"no artifacts to record for {fingerprint}")

        for artifact in artifacts:
            if not self._cas.exists(artifact.digest):
                raise ValidationError(
                    f"artifact {artifact.name!r} references a blob absent from the "
                    f"store: {artifact.digest}"
                )

        if self.lookup(fingerprint) is not None:
            return False

        now = utc_now_iso()
        with transaction(self._conn, immediate=True):
            for artifact in artifacts:
                self._conn.execute(
                    "INSERT INTO artifacts "
                    "(fingerprint, name, stage_id, kind, digest, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        fingerprint,
                        artifact.name,
                        stage_id,
                        artifact.kind,
                        artifact.digest,
                        now,
                    ),
                )
        for artifact in artifacts:
            self._cas.retain(artifact.digest)
        return True

    def forget(self, fingerprint: str) -> None:
        """Drop a fingerprint's artifacts and release their blobs. Idempotent.

        Raises:
            ValidationError: if ``fingerprint`` is malformed.
        """
        self._validate_fingerprint(fingerprint)
        existing = self.lookup(fingerprint)
        if existing is None:
            return
        self._drop_rows(fingerprint)
        for artifact in existing:
            self._cas.release(artifact.digest)
```

- [ ] **Step 4: Run to verify they pass**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_artifacts.py -v
```

Expected: 14 passed.

- [ ] **Step 5: Prove the double-retain guard is load-bearing**

Temporarily delete the `if self.lookup(fingerprint) is not None: return False` early return (leaving the insert to fail or succeed as it may). Confirm `test_recording_the_same_fingerprint_twice_does_not_inflate_refcounts` FAILS. Restore, confirm PASS. Paste both outputs.

- [ ] **Step 6: Run the full gate and confirm `doctor` still passes**

```powershell
powershell -ExecutionPolicy Bypass -File scripts/check.ps1
.\.venv\Scripts\ytauto.exe doctor; $LASTEXITCODE
```

Expected: `ALL CHECKS PASSED`; nine `[ OK ]` rows with `database  schema v2 (head v2)`; exit 0.

- [ ] **Step 7: Commit**

```bash
git add src/ytauto/infra/artifacts.py tests/unit/infra/test_artifacts.py
git commit -m "feat: add ArtifactStore mapping fingerprints to cached artifacts"
```

---

## Phase 1a Exit Checklist

- [ ] `scripts/check.ps1` passes: ruff, ruff format, mypy, import-linter, pytest (unit + integration)
- [ ] `ytauto doctor` still green, reporting `schema v2 (head v2)`
- [ ] `import-linter` still proves `core/` imports nothing internal and no layer below `ui/` imports Qt — the new `core/pipeline`, `core/ports` and `core/models` modules are stdlib-only
- [ ] A pipeline can be constructed, validated, topologically ordered, and asked what is downstream of a changed stage
- [ ] `compute_fingerprint` is proven stable across interpreter restarts under three different `PYTHONHASHSEED` values
- [ ] Every guard-pinning test has been demonstrated failing with its production guard deleted
- [ ] Every new public function carries a `Raises:` docstring section
- [ ] No `TODO` or `FIXME` on the shipped path

**Next:** Phase 1b — persistent job queue with claim-with-lease, resource governor (`gpu_compute` capacity **1**, a hard constant and not derived from `vram_mb`), JSON-lines worker protocol, stage runner, dispatcher with reaping, and the phase exit criterion: a synthetic three-stage job that runs, is killed mid-flight, and resumes from its last completed stage.
