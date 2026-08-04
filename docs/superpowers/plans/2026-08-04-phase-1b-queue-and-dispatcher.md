# Phase 1b — Queue, Governor, Worker Protocol and Dispatcher

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the Phase 1a domain libraries into a running scheduler: a persistent job queue with claim-with-lease, a resource governor, real subprocess workers speaking JSON-lines, and a dispatcher that reaps the dead and resumes from the last completed stage.

**Architecture:** Only the main process writes to SQLite; workers write blob *files* and report digests over a pipe, and the dispatcher owns every row. The artifact cache no longer pins blobs, so the disk ceiling is enforceable and a lost blob is simply a cache miss. `transaction()` becomes re-entrant via savepoints so "claim a job and pin its inputs" and "mark a stage done and record its artifacts" are each one atomic step.

**Tech Stack:** Python 3.12, stdlib only in `core/`, `sqlite3` in WAL mode, `subprocess` for workers, pytest.

Spec of record: `docs/superpowers/specs/2026-08-04-phase-1b-queue-and-dispatcher-design.md`.
Carry-forward: `docs/superpowers/phase-1a-carry-forward.md`.

## Global Constraints

Every task's requirements implicitly include this section.

- **`core/` imports nothing but the standard library.** No `infra`, `app`, `providers`, `ui`, `cli`, no `PySide6`, no third-party packages. `import-linter` fails the build on violation.
- **`app/` may import `core` and `infra/db` only** — not `providers`, not `ui`, not `cli`.
- **`ytauto.core.*` must pass `mypy --strict`.** Complete annotations on every function, parameter and return.
- **No bare `except`.** Every handler names concrete types.
- **Every public function and method in `core/**`, `infra/**` and `app/**` carries a `Raises:` docstring section** naming concrete exception types and when — or says nothing if it genuinely raises nothing.
- **Content hashes are SHA-256, lowercase hex, full 64 chars**, via `ytauto.core.models.content_hash`.
- **All timestamps UTC ISO-8601 with explicit `+00:00`**, via `ytauto.infra.clock.utc_now_iso()`. SQLite's `datetime('now')` is forbidden.
- **Fingerprints must be stable across processes and interpreter restarts.** No `hash()`, no `id()`, no reliance on dict insertion order, no absolute paths.
- Migrations are **append-only**. Never edit a released migration; add a new one.
- **`gpu_compute` pool capacity is the integer constant `1`.** Never derived from `vram_mb`. Deriving it invites a "4096 MiB, so 2 slots" mistake that produces exactly the nondeterministic VRAM exhaustion the governor exists to prevent.
- **Workers never import Qt and never write to SQLite.** Both are enforced, not merely documented.
- Test database connections must be closed so Windows can delete `tmp_path`.
- Test output pristine — no warnings.
- Run `.\.venv\Scripts\python.exe -m ruff format src tests` before the gate; `scripts/check.ps1` must print `ALL CHECKS PASSED`.
- **When a test's purpose is to pin a guard** — a `try/except`, a filter, a transaction wrapper, a validation branch — demonstrate it failing with the production guard deleted. State the *expected failure message*, not merely that it fails: Phase 1a shipped a proof that passed for an unrelated reason and let a wrong rationale into three docstrings.
- **If a predicted failure does not materialise, or materialises for a different reason, report it rather than smoothing it over.** Three implementers in Phase 1a found real defects exactly this way. This is the highest-yield instruction in this plan.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/ytauto/infra/db/engine.py` *(modify)* | SAVEPOINT re-entrancy; `TransactionError` narrows to the one case savepoints cannot serve |
| `src/ytauto/infra/db/migrations.py` *(modify)* | Migration 003: `available_at`, per-stage `attempts`, rebuilt claim index |
| `src/ytauto/infra/artifacts.py` *(modify)* | Cache stops pinning; `lookup` becomes a pure read; new `heal()` |
| `src/ytauto/infra/cas/store.py` *(modify)* | Split into worker-side `stage_file` and parent-side `record_blob` |
| `src/ytauto/core/pipeline/stage.py` *(modify)* | `StageResult` sorts artifacts by name |
| `src/ytauto/core/pipeline/fingerprint.py` *(modify)* | Artifact names enter the payload; schema version bumped |
| `src/ytauto/core/pipeline/graph.py` *(modify)* | `ready_stages()`, `upstream_of()` |
| `src/ytauto/core/models/names.py` *(create)* | One duplicate-name detector, replacing three |
| `src/ytauto/app/scheduler/worker_protocol.py` *(create)* | Versioned JSON-lines message schema |
| `src/ytauto/app/scheduler/queue.py` *(create)* | Claim-with-lease over `jobs` |
| `src/ytauto/app/scheduler/governor.py` *(create)* | Lease broker; `gpu_compute` capacity 1 |
| `src/ytauto/app/scheduler/runner.py` *(create)* | Executes one stage; database-pure |
| `src/ytauto/app/scheduler/dispatcher.py` *(create)* | Claim, spawn, pump, reap, commit |
| `src/ytauto/app/worker.py` *(create)* | Subprocess entry point |

---

## Task 1: `transaction()` becomes re-entrant via savepoints

**Files:**
- Modify: `src/ytauto/infra/db/engine.py`
- Test: `tests/unit/infra/test_db_engine.py`

**Interfaces:**
- Consumes: `TransactionError` from `ytauto.core.errors`
- Produces: `transaction(conn, *, immediate: bool = False)` — unchanged signature, now nestable

Phase 0's carry-forward §1.2 asked for this and it has been open two phases. Phase 1b needs "claim job + retain inputs" and "mark stage succeeded + record artifacts" to each be one atomic step, and today they cannot be.

**The rule, exactly:**

| State | Behaviour |
|---|---|
| no open transaction | `BEGIN` / `COMMIT` / `ROLLBACK` (unchanged) |
| open transaction, `immediate=False` | `SAVEPOINT _sp_N` / `RELEASE _sp_N` / `ROLLBACK TO _sp_N` |
| open transaction, `immediate=True` | raise `TransactionError` |

That last row is not an oversight. A nested `immediate=True` **cannot** deliver immediate semantics — the write-lock timing was already fixed by the outer `BEGIN`. Silently downgrading it would reintroduce the `SQLITE_BUSY_SNAPSHOT` failure that `immediate=` exists to prevent.

`ROLLBACK TO` does **not** pop the savepoint, so the handler must `RELEASE` after rolling back or the savepoint stack leaks.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/infra/test_db_engine.py`:

```python
def test_a_nested_transaction_commits_through_a_savepoint(tmp_path: Path) -> None:
    """Re-entrancy is what lets 'claim a job and pin its inputs' be atomic."""
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE t (a TEXT)")

    with transaction(conn):
        conn.execute("INSERT INTO t VALUES ('outer')")
        with transaction(conn):
            conn.execute("INSERT INTO t VALUES ('inner')")

    assert [r["a"] for r in conn.execute("SELECT a FROM t ORDER BY a")] == ["inner", "outer"]
    conn.close()


def test_an_inner_failure_rolls_back_only_to_the_savepoint(tmp_path: Path) -> None:
    """The outer transaction must survive an inner failure and still commit.
    Without ROLLBACK TO, the inner failure would discard the outer work too."""
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE t (a TEXT)")

    with transaction(conn):
        conn.execute("INSERT INTO t VALUES ('outer')")
        with pytest.raises(ValueError):
            with transaction(conn):
                conn.execute("INSERT INTO t VALUES ('inner')")
                raise ValueError("stage failed")

    assert [r["a"] for r in conn.execute("SELECT a FROM t")] == ["outer"]
    conn.close()


def test_nesting_immediate_inside_a_deferred_transaction_is_refused(tmp_path: Path) -> None:
    """A nested immediate=True cannot deliver immediate semantics - the write
    lock timing was already decided by the outer BEGIN. Downgrading it silently
    would reintroduce the SQLITE_BUSY_SNAPSHOT failure immediate= prevents."""
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE t (a TEXT)")

    with transaction(conn):
        with pytest.raises(TransactionError, match="immediate"):
            with transaction(conn, immediate=True):
                pass
    conn.close()


def test_savepoints_nest_more_than_one_deep(tmp_path: Path) -> None:
    """Names come from a depth counter, so siblings and nested savepoints must
    not collide - a single reused name would make the inner RELEASE pop the
    wrong frame."""
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE t (a TEXT)")

    with transaction(conn):
        with transaction(conn):
            with transaction(conn):
                conn.execute("INSERT INTO t VALUES ('deep')")
        with transaction(conn):
            conn.execute("INSERT INTO t VALUES ('sibling')")

    assert [r["a"] for r in conn.execute("SELECT a FROM t ORDER BY a")] == ["deep", "sibling"]
    conn.close()
```

- [ ] **Step 2: Run to verify they fail**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_db_engine.py -v -k "savepoint or nested or nesting_immediate"
```

Expected: the first, second and fourth FAIL with `TransactionError: a transaction is already open on this connection`. The third PASSES already (the current guard refuses all nesting) — note this, it is expected and means only its *reason* changes.

- [ ] **Step 3: Implement**

Replace the body of `transaction()` in `src/ytauto/infra/db/engine.py`:

```python
_SAVEPOINT_DEPTH: dict[int, int] = {}


@contextmanager
def transaction(
    conn: sqlite3.Connection, *, immediate: bool = False
) -> Iterator[sqlite3.Connection]:
    """Run a block in one transaction: commit on success, roll back on any error.

    Re-entrant. An outermost call issues BEGIN/COMMIT; a nested call issues a
    SAVEPOINT instead, so composing two modules that each open a transaction is
    safe and the whole composition still lands atomically.

    Pass ``immediate=True`` for read-then-write work such as claiming a queued
    job or acquiring a resource lease. A deferred ``BEGIN`` upgrades to a write
    lock lazily, and in WAL mode that upgrade returns SQLITE_BUSY_SNAPSHOT
    *immediately* without invoking the busy handler - so ``busy_timeout`` does
    not apply and the caller sees a spurious failure under concurrency.

    ``immediate=True`` is refused inside an existing transaction: the write-lock
    timing was already decided by the outer BEGIN, so honouring the flag is
    impossible and silently downgrading it would reintroduce the very failure it
    exists to prevent.

    Raises:
        TransactionError: if ``immediate=True`` is requested while a transaction
            is already open on ``conn``. Always a programming error, never
            retryable.
        sqlite3.OperationalError: if the write lock could not be acquired within
            ``busy_timeout`` - legitimate contention, which callers competing for
            a job or a lease must expect and handle.
        BaseException: anything raised inside the block, after rolling back.
    """
    key = id(conn)
    if conn.in_transaction:
        if immediate:
            raise TransactionError(
                "immediate=True cannot be honoured inside an open transaction; "
                "the write lock was already taken by the outer BEGIN - move the "
                "immediate transaction to the outermost call site"
            )
        depth = _SAVEPOINT_DEPTH.get(key, 0)
        name = f"_sp_{depth}"
        _SAVEPOINT_DEPTH[key] = depth + 1
        conn.execute(f"SAVEPOINT {name}")
        try:
            yield conn
        except BaseException:
            # ROLLBACK TO does not pop the savepoint; RELEASE must follow or the
            # stack leaks a frame per failure.
            conn.execute(f"ROLLBACK TO {name}")
            conn.execute(f"RELEASE {name}")
            _SAVEPOINT_DEPTH[key] = depth
            raise
        conn.execute(f"RELEASE {name}")
        _SAVEPOINT_DEPTH[key] = depth
        return

    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        _SAVEPOINT_DEPTH.pop(key, None)
        raise
    conn.execute("COMMIT")
    _SAVEPOINT_DEPTH.pop(key, None)
```

- [ ] **Step 4: Run to verify they pass**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_db_engine.py -v
```

Expected: all pass. The pre-existing `test_a_refused_nested_transaction_leaves_the_outer_one_intact` now describes obsolete behaviour — **rewrite it** to assert the `immediate=True` refusal instead of a blanket refusal, keeping its "outer transaction survives" assertion, which is still exactly right.

- [ ] **Step 5: Prove the RELEASE-after-ROLLBACK-TO is load-bearing**

Delete the `conn.execute(f"RELEASE {name}")` inside the `except` branch. Run `test_an_inner_failure_rolls_back_only_to_the_savepoint` and the deep-nesting test. Expected: a leaked savepoint frame; the depth counter and SQLite's stack disagree, and a later `RELEASE` pops the wrong frame. Paste the actual failure. Restore.

If this proof does **not** fail, say so — it would mean the savepoint stack is more forgiving than assumed, and the plan's claim is wrong.

- [ ] **Step 6: Gate and commit**

```powershell
.\.venv\Scripts\python.exe -m ruff format src tests
powershell -ExecutionPolicy Bypass -File scripts/check.ps1
```

```bash
git add src/ytauto/infra/db/engine.py tests/unit/infra/test_db_engine.py
git commit -m "feat: make transaction() re-entrant via savepoints"
```

---

## Task 2: Migration 003 — `available_at` and per-stage attempts

**Files:**
- Modify: `src/ytauto/infra/db/migrations.py`
- Test: `tests/unit/infra/test_migrations.py`

**Interfaces:**
- Produces: schema v3. `jobs.available_at TEXT NOT NULL DEFAULT ''`, `job_stages.attempts INTEGER NOT NULL DEFAULT 0`, index `idx_jobs_claimable` on `(state, available_at, priority DESC, created_at)`.

`ProviderError.retry_after_s` and `ErrorKind.RATE_LIMITED` exist precisely to defer work, and today the claim query has no way to honour them. `job_stages` has no per-stage attempt counter either, so one poison stage burns the whole job's budget.

The `''` default is deliberate: every timestamp here is ISO-8601, and `''` sorts before all of them, so existing rows are immediately claimable with no backfill.

- [ ] **Step 1: Write the failing tests**

```python
def test_migration_003_adds_available_at(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    cols = {r["name"]: r for r in conn.execute("PRAGMA table_info(jobs)")}
    assert "available_at" in cols
    assert cols["available_at"]["notnull"] == 1
    conn.close()


def test_migration_003_adds_per_stage_attempts(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(job_stages)")}
    assert "attempts" in cols
    conn.close()


def test_the_claim_index_leads_with_state_then_available_at(tmp_path: Path) -> None:
    """The claim query filters on state AND available_at before ordering. An
    index that does not lead with both cannot serve it as a covering scan."""
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    cols = [r["name"] for r in conn.execute("PRAGMA index_info(idx_jobs_claimable)")]
    assert cols[:2] == ["state", "available_at"]
    conn.close()


def test_head_version_is_three(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    assert conn.execute("SELECT max(version) FROM schema_version").fetchone()[0] == 3
    assert HEAD_VERSION == 3
    conn.close()


def test_existing_jobs_are_immediately_claimable_after_upgrade(tmp_path: Path) -> None:
    """The '' default must sort before every ISO-8601 timestamp, so rows written
    under v2 need no backfill to remain claimable."""
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    now = utc_now_iso()
    conn.execute(
        "INSERT INTO jobs (id, project_id, pipeline_id, state, created_at, updated_at) "
        "VALUES ('j1', 'p1', 'pipe', 'queued', ?, ?)",
        (now, now),
    )
    row = conn.execute(
        "SELECT id FROM jobs WHERE state = 'queued' AND available_at <= ?", (now,)
    ).fetchone()
    assert row["id"] == "j1"
    conn.close()
```

Add `HEAD_VERSION` and `utc_now_iso` to that module's imports.

- [ ] **Step 2: Run to verify they fail**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_migrations.py -v -k "003 or claim_index or head_version or immediately_claimable"
```

Expected: FAIL — `available_at` absent, `HEAD_VERSION == 2`.

- [ ] **Step 3: Implement**

Append to `src/ytauto/infra/db/migrations.py`, **without editing `_M001` or `_M002`**:

```python
_M003 = Migration(
    version=3,
    name="retry_scheduling",
    statements=(
        "ALTER TABLE jobs ADD COLUMN available_at TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE job_stages ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0",
        "DROP INDEX idx_jobs_claimable",
        "CREATE INDEX idx_jobs_claimable "
        "ON jobs (state, available_at, priority DESC, created_at)",
    ),
)

MIGRATIONS: tuple[Migration, ...] = (_M001, _M002, _M003)
```

Replace the existing `MIGRATIONS` line rather than adding a second one.

- [ ] **Step 4: Run to verify they pass**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_migrations.py -v
```

Expected: all pass. `doctor` must now report `schema v3 (head v3)`.

- [ ] **Step 5: Verify the upgrade path from a v2 database**

Build a database, stop after `_M002`, then apply the full set and confirm the columns appear and no data is lost. There is an existing upgrade-path test in this file that patches the module-global `MIGRATIONS`; follow that pattern rather than inventing another.

- [ ] **Step 6: Gate and commit**

```bash
git add src/ytauto/infra/db/migrations.py tests/unit/infra/test_migrations.py
git commit -m "feat: add migration 003 for retry scheduling"
```

---

## Task 3: The cache stops pinning

**Files:**
- Modify: `src/ytauto/infra/artifacts.py`
- Test: `tests/unit/infra/test_artifacts.py`, `tests/unit/infra/test_cas_eviction.py`

**Interfaces:**
- Produces: `ArtifactStore.record()` no longer retains; `forget()` no longer releases; `lookup()` no longer deletes; new `heal() -> int` returning the number of stale fingerprints reclaimed.

**This is the Critical from the whole-branch review.** `record()` retained every artifact and `forget()` had no caller, so `iter_evictable()` — which selects `WHERE refcount = 0` — could never see a cached blob. As the cache filled, the only remaining eviction candidates were blobs written but not yet recorded: the outputs of *running* stages. Measured on the merged branch, a 100-byte ceiling against 5300 bytes stored evicted exactly one blob, the in-flight one.

A cache entry whose blob was aged out is a **miss**, not a corruption, and `lookup()` already detects that. In-flight protection moves to the job, which retains what it will consume and releases on completion or reap.

Removing the retain also dissolves the stranded-refcount finding deferred from Task 10 of Phase 1a — nothing retains, so nothing can be stranded. Delete that paragraph from `_drop_rows`' docstring; it now describes an impossible state.

- [ ] **Step 1: Write the failing tests**

```python
def test_recording_does_not_pin_the_blob(artifacts: ArtifactStore, store: CasStore) -> None:
    """The cache must not pin. A cached blob that nothing else holds has to stay
    evictable, or the disk ceiling stops being enforceable and the evictor's only
    remaining candidates become the outputs of running stages."""
    ref = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [ref])
    assert store.refcount(ref.digest) == 0
    assert [d for d, _ in store.iter_evictable()] == [ref.digest]


def test_a_cached_artifact_is_evictable_and_the_ceiling_is_enforced(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    """Direct regression test for carry-forward 1.1."""
    ref = _put(store, "narration", b"x" * 1000)
    artifacts.record(FP, "tts", [ref])

    report = Evictor(store, EvictionPolicy(max_bytes=1)).run()

    assert report.evicted == 1
    assert not store.exists(ref.digest)
    assert artifacts.lookup(FP) is None, "an aged-out entry is a miss, not a hit"


def test_forget_does_not_release(artifacts: ArtifactStore, store: CasStore) -> None:
    """Symmetry: record() no longer retains, so forget() must not release, or
    it would drive the refcount below what other holders expect."""
    ref = _put(store, "narration", b"audio")
    store.retain(ref.digest)  # a job pins it
    artifacts.record(FP, "tts", [ref])
    artifacts.forget(FP)
    assert store.refcount(ref.digest) == 1, "the job's pin must survive forget()"


def test_lookup_does_not_delete_stale_rows(
    artifacts: ArtifactStore, store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """lookup() must be a pure read so the scheduler can probe the cache while
    holding a claim. It still reports a MISS - only the DELETE moves out."""
    ref = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [ref])
    store.path_for(ref.digest).unlink()

    assert artifacts.lookup(FP) is None, "a vanished blob is still a miss"
    assert _row_count(db_conn, FP) == 1, "but the row must survive the read"


def test_heal_reclaims_what_lookup_left_behind(
    artifacts: ArtifactStore, store: CasStore, db_conn: sqlite3.Connection
) -> None:
    ref = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [ref])
    store.path_for(ref.digest).unlink()

    assert artifacts.heal() == 1
    assert _row_count(db_conn, FP) == 0
    assert artifacts.heal() == 0, "idempotent"


def test_lookup_is_safe_inside_an_open_transaction(
    artifacts: ArtifactStore, store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """The scheduler claims a job and probes the cache in one transaction. Before
    this change the probe's self-healing DELETE took a write lock and rolled the
    caller's claim back."""
    ref = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [ref])
    store.path_for(ref.digest).unlink()

    with transaction(db_conn, immediate=True):
        db_conn.execute("INSERT INTO jobs (id, project_id, pipeline_id, state, "
                        "created_at, updated_at) VALUES ('j1','p1','pipe','running',?,?)",
                        (utc_now_iso(), utc_now_iso()))
        assert artifacts.lookup(FP) is None

    assert db_conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1
```

- [ ] **Step 2: Run to verify they fail**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/infra/test_artifacts.py -v -k "not_pin or evictable or does_not_release or does_not_delete or heal or inside_an_open"
```

Expected: `test_recording_does_not_pin_the_blob` FAILS with `assert 1 == 0`; `heal` FAILS with `AttributeError`; the transaction test FAILS with `TransactionError` (Task 1 having narrowed it) or `OperationalError`.

- [ ] **Step 3: Implement**

In `record()`, delete the trailing retain loop:

```python
        for artifact in artifacts:
            self._cas.retain(artifact.digest)
```

In `forget()`, delete the release loop and its `try/except ValidationError` entirely, leaving:

```python
        self._validate_fingerprint(fingerprint)
        if self.lookup(fingerprint) is None and not self._has_rows(fingerprint):
            return
        self._drop_rows(fingerprint)
```

In `lookup()`, replace `self._drop_rows(fingerprint)` with nothing — just `return None`.

Add:

```python
    def heal(self) -> int:
        """Drop rows whose blobs are gone. Returns the number of fingerprints cleared.

        ``lookup`` deliberately does not do this: it must stay a pure read so the
        scheduler can probe the cache while holding a job claim. Detection lives
        there, reclamation lives here, and the split is why a cache probe cannot
        take a write lock.

        Raises:
            sqlite3.OperationalError: if the delete cannot acquire the write lock
                within ``busy_timeout`` (legitimate contention).
            TransactionError: if ``immediate=True`` is requested inside an open
                transaction - do not call this from inside a claim.
        """
        rows = self._conn.execute("SELECT DISTINCT fingerprint FROM artifacts").fetchall()
        stale = [
            row["fingerprint"]
            for row in rows
            if not all(
                self._cas.exists(ContentHash(a["digest"]))
                for a in self._conn.execute(
                    "SELECT digest FROM artifacts WHERE fingerprint = ?", (row["fingerprint"],)
                )
            )
        ]
        for fingerprint in stale:
            self._drop_rows(fingerprint)
        return len(stale)
```

Update `record()`'s and `forget()`'s docstrings: the cache does not pin, and `forget()` drops rows only.

- [ ] **Step 4: Run to verify they pass**

Expect several *pre-existing* tests to fail — `test_record_retains_each_digest_once`, `test_forget_releases_and_removes`, `test_recording_the_same_fingerprint_twice_is_idempotent`'s refcount line, and `test_lookup_drops_the_stale_rows_it_finds`. Each asserts behaviour this task deliberately removes.

**Rewrite them, do not delete them.** `test_record_retains_each_digest_once` becomes `test_recording_does_not_pin_the_blob` (already written above — delete the old one). `test_lookup_drops_the_stale_rows_it_finds` becomes the `heal()` test. `test_forget_releases_and_removes` keeps its row assertions and drops its refcount assertion. Report exactly which tests you changed and why.

- [ ] **Step 5: Prove the ceiling regression test is load-bearing**

Restore the retain loop in `record()`. Expected: `test_a_cached_artifact_is_evictable_and_the_ceiling_is_enforced` FAILS with `assert 0 == 1` — the evictor freed nothing. Restore.

- [ ] **Step 6: Gate and commit**

```bash
git add src/ytauto/infra/artifacts.py tests/unit/infra/test_artifacts.py tests/unit/infra/test_cas_eviction.py
git commit -m "fix: stop the artifact cache pinning its blobs"
```

---

## Task 4: One duplicate-name detector

**Files:**
- Create: `src/ytauto/core/models/names.py`
- Modify: `src/ytauto/core/pipeline/graph.py`, `src/ytauto/core/pipeline/stage.py`, `src/ytauto/infra/artifacts.py`
- Test: `tests/unit/core/test_names.py`

**Interfaces:**
- Produces: `assert_unique_names(names: Iterable[str], *, what: str, context: str) -> None`

Three implementations of one rule exist today, two of them O(n²) (`names.count` inside a comprehension) and all three with different message shapes. The whole-branch review promoted this from a performance nit precisely because it is three implementations, not because of the complexity.

- [ ] **Step 1: Write the failing test**

```python
import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.models.names import assert_unique_names


def test_unique_names_pass() -> None:
    assert_unique_names(["a", "b", "c"], what="stage", context="pipeline 'p'")


def test_a_duplicate_is_named_in_the_message() -> None:
    with pytest.raises(ValidationError, match="duplicate stage name in pipeline 'p': 'b'"):
        assert_unique_names(["a", "b", "b"], what="stage", context="pipeline 'p'")


def test_the_first_duplicate_is_reported_not_the_last() -> None:
    """Deterministic messages matter: a test asserting on the message must not
    depend on which duplicate happens to be found."""
    with pytest.raises(ValidationError, match="'b'"):
        assert_unique_names(["a", "b", "b", "c", "c"], what="stage", context="p")


def test_an_empty_iterable_is_fine() -> None:
    assert_unique_names([], what="artifact", context="stage 'tts'")
```

- [ ] **Step 2: Run to verify it fails**

Expected: `ModuleNotFoundError: No module named 'ytauto.core.models.names'`.

- [ ] **Step 3: Implement**

```python
"""One duplicate-name check, shared by everything that needs it.

Three separate implementations existed before this, two of them quadratic and
all three with different message shapes, so the same violation read differently
depending on which layer caught it.
"""

from __future__ import annotations

from collections.abc import Iterable

from ytauto.core.errors import ValidationError


def assert_unique_names(names: Iterable[str], *, what: str, context: str) -> None:
    """Raise if any name repeats.

    Args:
        names: the names to check, in order.
        what: singular noun for the thing being named, e.g. ``"stage"``.
        context: where the collision happened, e.g. ``"pipeline 'intro'"``.

    Raises:
        ValidationError: if a name appears more than once. The message names the
            first repeat in iteration order, so it is deterministic.
    """
    seen: set[str] = set()
    for name in names:
        if name in seen:
            raise ValidationError(f"duplicate {what} name in {context}: {name!r}")
        seen.add(name)
```

- [ ] **Step 4: Run to verify it passes**

- [ ] **Step 5: Replace all three call sites**

In `graph.py`'s `Pipeline.__post_init__`, `stage.py`'s `StageResult.__post_init__`, and `artifacts.py`'s `record()`, delete the local detection and call `assert_unique_names`. Run the full unit suite — several existing tests assert on the *old* message shapes and will fail. Update those assertions to the new shape and **list every test you touched** in your report.

- [ ] **Step 6: Gate and commit**

```bash
git add src/ytauto/core/models/names.py tests/unit/core/test_names.py src/ytauto/core/pipeline/graph.py src/ytauto/core/pipeline/stage.py src/ytauto/infra/artifacts.py
git commit -m "refactor: consolidate three duplicate-name detectors into one"
```

---

## Task 5: `StageResult` sorts its artifacts by name

**Files:**
- Modify: `src/ytauto/core/pipeline/stage.py`
- Test: `tests/unit/core/test_stage.py`

**Interfaces:**
- Produces: `StageResult.artifacts` is always name-sorted, whatever order it was constructed with.

**This is the finding that would have let the phase's exit criterion pass while the caching it demonstrates was broken.** `StageResult.artifacts` was declaration order; `ArtifactStore.lookup()` returns `ORDER BY name ASC`; nothing reconciled them; and `FingerprintSpec.input_digests` is ordered *by design*. Measured: the same two artifacts produced downstream fingerprints `844c5bc9…` fresh versus `7a4fcd46…` cached.

Killed in stage 2 and resuming at stage 2, stage 3 was never cached — so the drift is invisible to a resume test. Run the same job twice and every downstream stage re-runs.

A stage needing a specific concatenation order encodes it in the names (`seg_000`, `seg_001`), which sorts correctly and is self-documenting.

- [ ] **Step 1: Write the failing tests**

```python
def test_artifacts_are_sorted_by_name(fake_digest: ContentHash) -> None:
    """Declaration order must equal name order, because ArtifactStore.lookup
    returns name order and a stage's fresh and cached paths must agree."""
    result = StageResult(
        artifacts=(
            ArtifactRef(name="timings", kind="blob", digest=fake_digest),
            ArtifactRef(name="narration", kind="blob", digest=fake_digest),
        ),
        meta={},
    )
    assert [a.name for a in result.artifacts] == ["narration", "timings"]


def test_a_stage_declaring_reverse_order_still_round_trips(fake_digest: ContentHash) -> None:
    """The property that matters: whatever order the stage declared, the tuple
    matches what a later lookup() will hand back."""
    declared = StageResult(
        artifacts=(
            ArtifactRef(name="zulu", kind="blob", digest=fake_digest),
            ArtifactRef(name="alpha", kind="blob", digest=fake_digest),
        ),
        meta={},
    )
    assert [a.name for a in declared.artifacts] == sorted(a.name for a in declared.artifacts)
```

- [ ] **Step 2: Run to verify they fail**

Expected: `assert ['timings', 'narration'] == ['narration', 'timings']`.

- [ ] **Step 3: Implement**

In `StageResult.__post_init__`, after the uniqueness check, sort. `StageResult` is a frozen dataclass, so use `object.__setattr__`:

```python
        object.__setattr__(
            self, "artifacts", tuple(sorted(self.artifacts, key=lambda a: a.name))
        )
```

Document it on the class: sorting is what keeps the fresh and cached paths from diverging, and stages that need a specific order encode it in the names.

- [ ] **Step 4: Run to verify they pass**

- [ ] **Step 5: Prove it end-to-end against the real drift**

Write the test that actually reproduces the original bug — it belongs in `tests/unit/infra/test_artifacts.py`:

```python
def test_declaration_order_and_cached_order_produce_one_fingerprint(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    """The regression the whole-branch review found: a downstream stage must
    fingerprint identically whether its inputs came fresh from StageResult or
    back from the cache."""
    timings = _put(store, "timings", b"json")
    narration = _put(store, "narration", b"audio")
    fresh = StageResult(artifacts=(timings, narration), meta={}).artifacts
    artifacts.record(FP, "tts", list(fresh))
    cached = artifacts.lookup(FP) or ()

    def downstream(arts: tuple[ArtifactRef, ...]) -> str:
        return compute_fingerprint(
            FingerprintSpec(
                stage_id="render", stage_version=1, provider_id="ffmpeg",
                provider_version="7.1",
                input_digests=tuple(a.digest for a in arts), settings={},
            )
        )

    assert downstream(fresh) == downstream(cached)
```

Then remove the sort and confirm this FAILS with two different 64-char hex strings. Restore. Paste both outputs — this is the proof that the phase's second exit criterion is real.

- [ ] **Step 6: Gate and commit**

```bash
git add src/ytauto/core/pipeline/stage.py tests/unit/core/test_stage.py tests/unit/infra/test_artifacts.py
git commit -m "fix: sort StageResult artifacts so fresh and cached paths agree"
```

---

## Task 6: Artifact names enter the fingerprint

**Files:**
- Modify: `src/ytauto/core/pipeline/fingerprint.py`
- Test: `tests/unit/core/test_fingerprint.py`

**Interfaces:**
- Produces: `FingerprintSpec.input_digests: tuple[tuple[str, ContentHash], ...]` — now `(name, digest)` pairs. `FINGERPRINT_SCHEMA_VERSION: int = 2`.

Today two upstream artifacts that swap names while keeping their digests fingerprint identically. That is a false cache **hit** — the pipeline skips a stage and serves a different arrangement of inputs as its output. Every other guard in this module defends against false *misses*, which only waste work.

Bumping the schema version invalidates every existing fingerprint. That is correct and intended: fingerprints computed without names are not trustworthy. The first run after this recomputes everything.

- [ ] **Step 1: Write the failing tests**

```python
def test_swapping_two_artifact_names_changes_the_fingerprint() -> None:
    """A false HIT is the most damaging bug this module can have: the pipeline
    skips a stage and serves a different arrangement of inputs as its output."""
    a, b = ContentHash("a" * 64), ContentHash("b" * 64)
    one = _spec(input_digests=(("narration", a), ("timings", b)))
    two = _spec(input_digests=(("narration", b), ("timings", a)))
    assert compute_fingerprint(one) != compute_fingerprint(two)


def test_the_same_names_and_digests_fingerprint_identically() -> None:
    a, b = ContentHash("a" * 64), ContentHash("b" * 64)
    assert compute_fingerprint(_spec(input_digests=(("n", a), ("t", b)))) == compute_fingerprint(
        _spec(input_digests=(("n", a), ("t", b)))
    )


def test_input_order_still_changes_the_fingerprint() -> None:
    """Order stays load-bearing - concatenating two clips the other way round
    produces a different video."""
    a, b = ContentHash("a" * 64), ContentHash("b" * 64)
    assert compute_fingerprint(_spec(input_digests=(("x", a), ("y", b)))) != compute_fingerprint(
        _spec(input_digests=(("y", b), ("x", a)))
    )


def test_schema_version_is_two_and_typed() -> None:
    assert FINGERPRINT_SCHEMA_VERSION == 2
    assert isinstance(FINGERPRINT_SCHEMA_VERSION, int)
```

Write `_spec(**overrides)` as a module-level helper returning a `FingerprintSpec` with sensible defaults, so each test states only what it varies.

- [ ] **Step 2: Run to verify they fail**

Expected: the name-swap test FAILS — both fingerprints identical.

- [ ] **Step 3: Implement**

Change the annotation and the payload:

```python
FINGERPRINT_SCHEMA_VERSION: int = 2
```

```python
    input_digests: tuple[tuple[str, ContentHash], ...]
```

```python
        "input_digests": [[name, digest] for name, digest in spec.input_digests],
```

Update `FingerprintSpec`'s docstring: the tuple is ordered because order changes the result, and each entry carries its artifact name because two artifacts swapping names while keeping digests would otherwise be indistinguishable.

- [ ] **Step 4: Run to verify they pass**

Existing `input_digests` call sites in tests pass bare digest tuples and will fail to type-check. Update them to `(name, digest)` pairs. Report every file you touched.

- [ ] **Step 5: Prove the schema version is load-bearing**

Monkeypatch `FINGERPRINT_SCHEMA_VERSION` to `99` and assert the fingerprint changes. `compute_fingerprint` reads the module global at call time, so the patch takes effect. This test already exists from Phase 1a — confirm it still passes and still asserts on `compute_fingerprint`, not on `canonical_json`.

- [ ] **Step 6: Gate and commit**

```bash
git add src/ytauto/core/pipeline/fingerprint.py tests/unit/core/test_fingerprint.py
git commit -m "fix: put artifact names in the fingerprint to close a false-hit"
```

---

## Task 7: `Pipeline` answers the scheduler's questions

**Files:**
- Modify: `src/ytauto/core/pipeline/graph.py`
- Test: `tests/unit/core/test_graph.py`

**Interfaces:**
- Produces: `Pipeline.ready_stages(done: frozenset[str]) -> tuple[Stage, ...]`, `Pipeline.upstream_of(stage_id: str) -> frozenset[str]`

`Pipeline` answers a sequential runner's questions, not a scheduler's. It cannot say which stages are ready given a done-set, and `topological_order()` is a *total* order that hides which stages are independent — the exact information a governor with `gpu_compute` capacity 1 exists to arbitrate, since the whole point is running non-GPU stages alongside the one GPU stage.

`upstream_of` is the mirror of the existing `downstream_of` and is what populates `JobContext.inputs`.

- [ ] **Step 1: Write the failing tests**

```python
def test_ready_stages_with_nothing_done_returns_the_roots() -> None:
    p = _diamond()  # a -> b, a -> c, (b, c) -> d
    assert [s.id for s in p.ready_stages(frozenset())] == ["a"]


def test_ready_stages_returns_independent_stages_together() -> None:
    """The parallelism the governor needs to see. topological_order() flattens
    b and c into an arbitrary sequence and cannot reveal they are independent."""
    p = _diamond()
    assert [s.id for s in p.ready_stages(frozenset({"a"}))] == ["b", "c"]


def test_a_stage_is_not_ready_until_every_dependency_is_done() -> None:
    p = _diamond()
    assert [s.id for s in p.ready_stages(frozenset({"a", "b"}))] == ["c"]
    assert [s.id for s in p.ready_stages(frozenset({"a", "b", "c"}))] == ["d"]


def test_a_done_stage_is_never_ready_again() -> None:
    p = _diamond()
    assert p.ready_stages(frozenset({"a", "b", "c", "d"})) == ()


def test_ready_stages_is_ordered_by_id() -> None:
    """Deterministic, so a dispatcher's choice under capacity pressure is
    reproducible across runs."""
    p = _diamond()
    assert [s.id for s in p.ready_stages(frozenset({"a"}))] == sorted(["c", "b"])


def test_upstream_of_is_transitive() -> None:
    p = _diamond()
    assert p.upstream_of("d") == frozenset({"a", "b", "c"})
    assert p.upstream_of("a") == frozenset()


def test_upstream_of_rejects_an_unknown_stage() -> None:
    with pytest.raises(ValidationError, match="unknown stage"):
        _diamond().upstream_of("nope")
```

Write `_diamond()` as a module-level helper building `a → (b, c) → d`.

- [ ] **Step 2: Run to verify they fail**

Expected: `AttributeError: 'Pipeline' object has no attribute 'ready_stages'`.

- [ ] **Step 3: Implement**

```python
    def ready_stages(self, done: frozenset[str]) -> tuple[Stage, ...]:
        """Stages whose dependencies are all satisfied and which are not yet done.

        ``topological_order`` gives a total order and so cannot express that two
        stages are independent. The governor needs exactly that: with
        ``gpu_compute`` capacity 1, the point is to run non-GPU stages alongside
        the single GPU stage. Ordered by stage id so a dispatcher's choice under
        capacity pressure is reproducible.

        Raises:
            None.
        """
        return tuple(
            stage
            for stage in sorted(self.stages, key=lambda s: s.id)
            if stage.id not in done and set(stage.depends_on) <= done
        )

    def upstream_of(self, stage_id: str) -> frozenset[str]:
        """Every stage ``stage_id`` transitively depends on. The mirror of downstream_of.

        This is what populates ``JobContext.inputs`` on a resume: the runner has
        to gather the artifacts of everything upstream, not just direct parents.

        Raises:
            ValidationError: if ``stage_id`` names no stage in this pipeline.
        """
        if stage_id not in self._by_id:
            raise ValidationError(f"unknown stage {stage_id!r} in pipeline {self.id!r}")
        seen: set[str] = set()
        frontier = list(self._by_id[stage_id].depends_on)
        while frontier:
            current = frontier.pop()
            if current in seen:
                continue
            seen.add(current)
            frontier.extend(self._by_id[current].depends_on)
        return frozenset(seen)
```

- [ ] **Step 4: Run to verify they pass**

- [ ] **Step 5: Prove `ready_stages` genuinely tests the dependency check**

Change `set(stage.depends_on) <= done` to `bool(set(stage.depends_on) & done)`. Expected: `test_a_stage_is_not_ready_until_every_dependency_is_done` FAILS, reporting `d` ready after only `b` is done. Restore.

Also fix `topological_order()`'s docstring while here: it says "ties broken by stage ID" but returns DFS post-order. Verified in review — stages `a→d`, `b`, `d` yield `['d','a','b']`, not the lexicographically-smallest `['b','d','a']`. It is deterministic, which is the property that matters; the docstring simply describes a different algorithm and would cause a Phase 1b test to pin the wrong thing.

- [ ] **Step 6: Gate and commit**

```bash
git add src/ytauto/core/pipeline/graph.py tests/unit/core/test_graph.py
git commit -m "feat: add ready_stages and upstream_of for the scheduler"
```

---

*Tasks 8–14 (CasStore split, worker protocol, queue, governor, runner, dispatcher, exit criteria) follow in the next section of this document.*
