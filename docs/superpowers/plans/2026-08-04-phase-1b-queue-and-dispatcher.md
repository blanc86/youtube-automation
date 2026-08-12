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

## Task 8: Split `CasStore` for the single-writer model

**Files:**
- Modify: `src/ytauto/infra/cas/store.py`
- Test: `tests/unit/infra/test_cas_store.py`

**Interfaces:**
- Produces: `stage_file(data: bytes, *, kind: str) -> ContentHash` (worker side, filesystem only, no SQLite); `record_blob(digest: ContentHash, *, kind: str, size_bytes: int) -> None` (parent side, row only, composable inside a caller's transaction).

Only the main process writes to SQLite. Workers write blob *files* and report digests over the pipe; the dispatcher owns every row. `put_bytes` did both, so it cannot be called from a worker.

The existing per-process staging paths (`{hash}.{pid}.tmp`) were built for exactly this and stay. `put_bytes` remains, now implemented as `stage_file` followed by `record_blob`, so existing callers and tests are untouched.

`record_blob` must **not** open its own transaction when one is already open — Task 1 makes that safe automatically, which is what lets the dispatcher do `record_blob` + `retain` + `ArtifactStore.record` + the `job_stages` update in one atomic step.

- [ ] **Step 1: Write the failing tests**

```python
def test_stage_file_writes_the_blob_without_touching_the_database(
    store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """A worker must be able to produce a blob with no SQLite write at all."""
    digest = store.stage_file(b"payload", kind="blob")
    assert store.path_for(digest).is_file()
    assert db_conn.execute("SELECT count(*) FROM cas_objects").fetchone()[0] == 0


def test_record_blob_creates_the_row_for_an_already_staged_file(store: CasStore) -> None:
    digest = store.stage_file(b"payload", kind="blob")
    store.record_blob(digest, kind="blob", size_bytes=len(b"payload"))
    assert store.refcount(digest) == 0
    assert store.size_of(digest) == len(b"payload")


def test_record_blob_is_idempotent(store: CasStore) -> None:
    """Cross-project dedup means the same digest is recorded more than once."""
    digest = store.stage_file(b"payload", kind="blob")
    store.record_blob(digest, kind="blob", size_bytes=7)
    store.record_blob(digest, kind="blob", size_bytes=7)
    assert store.refcount(digest) == 0


def test_record_blob_rejects_a_digest_with_no_file(store: CasStore) -> None:
    """The row must never outlive the file - a row without a file makes
    total_size() overcount and read_bytes() fail for a digest size_of answers."""
    with pytest.raises(ValidationError, match="no staged file"):
        store.record_blob(ContentHash("d" * 64), kind="blob", size_bytes=1)


def test_record_blob_joins_an_open_transaction(
    store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """The dispatcher records blobs, retains them, records artifacts and updates
    job_stages in ONE transaction. If record_blob could not join, that
    composition would be impossible."""
    digest = store.stage_file(b"payload", kind="blob")
    with pytest.raises(ValueError):
        with transaction(db_conn, immediate=True):
            store.record_blob(digest, kind="blob", size_bytes=7)
            raise ValueError("caller aborted")
    assert db_conn.execute("SELECT count(*) FROM cas_objects").fetchone()[0] == 0


def test_put_bytes_still_works_and_is_stage_plus_record(store: CasStore) -> None:
    digest = store.put_bytes(b"payload", kind="blob")
    assert store.exists(digest)
    assert store.size_of(digest) == 7
```

- [ ] **Step 2: Run to verify they fail**

Expected: `AttributeError: 'CasStore' object has no attribute 'stage_file'`.

- [ ] **Step 3: Implement**

Extract the file-writing half of `put_bytes` into `stage_file` and the row-writing half into `record_blob`, then reimplement `put_bytes` in terms of both. `record_blob` uses the existing `ON CONFLICT DO UPDATE SET last_accessed_at = ...` for idempotence, and checks `self.path_for(digest).is_file()` first so a row can never outlive its file.

Give all three the project's `Raises:` sections. `stage_file` raises `OSError`; `record_blob` raises `ValidationError` for a malformed digest or a missing file, `sqlite3.OperationalError` under contention, and `TransactionError` only for a nested `immediate=True` — which it does not request, so it should say nothing about that.

- [ ] **Step 4: Run to verify they pass**

- [ ] **Step 5: Prove the file-first ordering guard is load-bearing**

Delete the `is_file()` check in `record_blob`. Expected: `test_record_blob_rejects_a_digest_with_no_file` FAILS with `DID NOT RAISE ValidationError`, and a row now exists for a digest with no file. Restore.

- [ ] **Step 6: Gate and commit**

```bash
git add src/ytauto/infra/cas/store.py tests/unit/infra/test_cas_store.py
git commit -m "feat: split CasStore into worker-side staging and parent-side recording"
```

---

## Task 9: The worker protocol

**Files:**
- Create: `src/ytauto/app/scheduler/__init__.py`, `src/ytauto/app/scheduler/worker_protocol.py`
- Test: `tests/unit/app/test_worker_protocol.py`

**Interfaces:**
- Produces: `PROTOCOL_VERSION: int = 1`; frozen dataclasses `Progress`, `Staged`, `Result`, `Error`, `LogLine`; `encode(msg: Message) -> str`; `decode(line: str) -> Message | None`.

One JSON object per line on the worker's stdout. Every message carries `v`, `type`, `job_id`, `stage_id` and `correlation_id`.

`correlation_id` is an **explicit field**, not read from a `ContextVar`. Phase 0's carry-forward §1.3 established that a relayed log line otherwise gets stamped with the *parent's* ID, quietly destroying the per-job trail that is the whole point of the mechanism.

`decode` returns `None` for an unknown `type` or an unknown `v` rather than raising, so a newer worker cannot wedge an older parent. It raises only for a line that is not JSON at all — that means the pipe is corrupt, which is not survivable.

- [ ] **Step 1: Write the failing tests**

```python
def test_a_result_round_trips() -> None:
    msg = Result(
        job_id="j1", stage_id="tts", correlation_id="c1",
        artifacts=(ArtifactLine(name="narration", kind="blob", digest="a" * 64),),
        meta={"duration_s": 12.5},
    )
    assert decode(encode(msg)) == msg


def test_every_message_carries_its_correlation_id() -> None:
    """Read from a ContextVar instead and a relayed line gets the PARENT's id,
    destroying the per-job trail - carry-forward 1.3."""
    line = encode(Progress(job_id="j1", stage_id="tts", correlation_id="c1",
                           fraction=0.5, note="synthesising"))
    assert json.loads(line)["correlation_id"] == "c1"


def test_encode_emits_exactly_one_line() -> None:
    """The transport is line-delimited; an embedded newline would split one
    message into two unparseable halves."""
    line = encode(LogLine(job_id="j1", stage_id="tts", correlation_id="c1",
                          level="ERROR", message="boom\nsecond line", exc=None))
    assert line.count("\n") == 0
    assert decode(line).message == "boom\nsecond line"


def test_an_unknown_message_type_decodes_to_none() -> None:
    """A newer worker must not be able to wedge an older parent."""
    assert decode(json.dumps({"v": 1, "type": "telemetry", "job_id": "j1",
                              "stage_id": "s", "correlation_id": "c"})) is None


def test_an_unknown_protocol_version_decodes_to_none() -> None:
    assert decode(json.dumps({"v": 99, "type": "progress", "job_id": "j1",
                              "stage_id": "s", "correlation_id": "c",
                              "fraction": 0.1, "note": ""})) is None


def test_a_non_json_line_is_fatal() -> None:
    """A corrupt pipe is not survivable and must not be silently skipped."""
    with pytest.raises(ValidationError, match="not valid JSON"):
        decode("this is not json")


def test_an_error_carries_its_kind_and_retry_hint() -> None:
    msg = Error(job_id="j1", stage_id="tts", correlation_id="c1",
                message="429 from provider", kind=ErrorKind.RATE_LIMITED,
                retry_after_s=30.0)
    assert decode(encode(msg)) == msg
    assert decode(encode(msg)).kind is ErrorKind.RATE_LIMITED
```

- [ ] **Step 2: Run to verify they fail**

Expected: `ModuleNotFoundError: No module named 'ytauto.app.scheduler.worker_protocol'`.

- [ ] **Step 3: Implement**

Frozen dataclasses for each message, a `Message` union alias, `encode` using `json.dumps(..., separators=(",", ":"))` (which escapes embedded newlines, satisfying the one-line requirement), and `decode` dispatching on `type` after checking `v`.

`ErrorKind` serialises by value — it is a `StrEnum`, so `kind.value` round-trips through `ErrorKind(raw)`.

- [ ] **Step 4: Run to verify they pass**

- [ ] **Step 5: Prove the version guard is load-bearing**

Delete the `v` check in `decode`. Expected: `test_an_unknown_protocol_version_decodes_to_none` FAILS — a v99 `progress` decodes into a v1 `Progress` rather than being ignored. Restore.

- [ ] **Step 6: Gate and commit**

```bash
git add src/ytauto/app/scheduler/__init__.py src/ytauto/app/scheduler/worker_protocol.py tests/unit/app/test_worker_protocol.py
git commit -m "feat: add the versioned JSON-lines worker protocol"
```

---

## Task 10: The job queue

**Files:**
- Create: `src/ytauto/app/scheduler/queue.py`
- Test: `tests/unit/app/test_queue.py`

**Interfaces:**
- Produces: `JobQueue(conn)` with `enqueue(job_id, project_id, pipeline_id, *, priority=0) -> None`, `claim(owner, *, lease_s) -> ClaimedJob | None`, `renew(job_id, owner, *, lease_s) -> bool`, `requeue(job_id, *, available_in_s=0.0, error=None) -> None`, `complete(job_id) -> None`, `fail(job_id, error) -> None`, `reap_expired(*, now=None) -> tuple[str, ...]`.

`claim` is the read-then-write that `immediate=True` exists for. In WAL mode a deferred transaction that reads and then writes gets `SQLITE_BUSY_SNAPSHOT` returned immediately, without the busy handler running, so `busy_timeout` does not apply — a flaky, load-dependent queue bug that only shows under concurrency.

- [ ] **Step 1: Write the failing tests**

```python
def test_claim_returns_none_on_an_empty_queue(queue: JobQueue) -> None:
    assert queue.claim("w1", lease_s=60) is None


def test_enqueue_then_claim_round_trips(queue: JobQueue) -> None:
    queue.enqueue("j1", "p1", "pipe")
    claimed = queue.claim("w1", lease_s=60)
    assert claimed is not None
    assert claimed.job_id == "j1"
    assert claimed.attempts == 1


def test_only_one_of_two_claimers_wins(queue: JobQueue) -> None:
    """Two workers racing for one job. The loser must get None, not the same job."""
    queue.enqueue("j1", "p1", "pipe")
    first = queue.claim("w1", lease_s=60)
    second = queue.claim("w2", lease_s=60)
    assert first is not None and second is None


def test_higher_priority_is_claimed_first(queue: JobQueue) -> None:
    queue.enqueue("low", "p1", "pipe", priority=0)
    queue.enqueue("high", "p1", "pipe", priority=10)
    assert queue.claim("w1", lease_s=60).job_id == "high"


def test_a_job_deferred_by_available_at_is_not_claimable(queue: JobQueue) -> None:
    """This is what makes ErrorKind.RATE_LIMITED and retry_after_s real."""
    queue.enqueue("j1", "p1", "pipe")
    queue.requeue("j1", available_in_s=3600)
    assert queue.claim("w1", lease_s=60) is None


def test_a_deferred_job_becomes_claimable_once_its_time_passes(queue: JobQueue) -> None:
    queue.enqueue("j1", "p1", "pipe")
    queue.requeue("j1", available_in_s=-1)  # already due
    assert queue.claim("w1", lease_s=60) is not None


def test_an_expired_lease_is_reaped_and_the_job_returns_to_the_queue(
    queue: JobQueue
) -> None:
    queue.enqueue("j1", "p1", "pipe")
    queue.claim("w1", lease_s=-1)  # already expired
    assert queue.reap_expired() == ("j1",)
    assert queue.claim("w2", lease_s=60) is not None


def test_a_live_lease_is_not_reaped(queue: JobQueue) -> None:
    queue.enqueue("j1", "p1", "pipe")
    queue.claim("w1", lease_s=3600)
    assert queue.reap_expired() == ()


def test_renew_extends_only_the_owner_s_lease(queue: JobQueue) -> None:
    """A worker that lost its job to the reaper must not be able to renew it."""
    queue.enqueue("j1", "p1", "pipe")
    queue.claim("w1", lease_s=60)
    assert queue.renew("j1", "w1", lease_s=120) is True
    assert queue.renew("j1", "impostor", lease_s=120) is False


def test_attempts_increments_on_every_claim(queue: JobQueue) -> None:
    queue.enqueue("j1", "p1", "pipe")
    queue.claim("w1", lease_s=-1)
    queue.reap_expired()
    assert queue.claim("w2", lease_s=60).attempts == 2


def test_complete_and_fail_are_terminal(queue: JobQueue) -> None:
    queue.enqueue("j1", "p1", "pipe")
    queue.claim("w1", lease_s=60)
    queue.complete("j1")
    assert queue.claim("w2", lease_s=60) is None
```

Provide a `queue` fixture built on the existing `db_conn`.

- [ ] **Step 2: Run to verify they fail**

Expected: `ModuleNotFoundError: No module named 'ytauto.app.scheduler.queue'`.

- [ ] **Step 3: Implement**

`claim` in one `transaction(conn, immediate=True)`: `SELECT id, attempts FROM jobs WHERE state='queued' AND available_at <= ? ORDER BY priority DESC, created_at LIMIT 1`, then `UPDATE ... SET state='running', lease_owner=?, lease_expires_at=?, attempts=attempts+1, updated_at=?`. Return a frozen `ClaimedJob(job_id, project_id, pipeline_id, attempts)`.

`reap_expired` selects `WHERE state='running' AND lease_expires_at < ?` and returns them to `queued`, clearing the lease. `renew` updates only `WHERE id=? AND lease_owner=?` and returns `cursor.rowcount == 1`.

All timestamps via `utc_now_iso()`. Never `datetime('now')`.

- [ ] **Step 4: Run to verify they pass**

- [ ] **Step 5: Prove `immediate=True` and the owner check are load-bearing**

Two proofs:

(a) Change `claim`'s `immediate=True` to `False`. State honestly what happens: with a single connection this likely still passes, because the race needs two connections contending. If it does not fail, **say so** and add a second-connection test that does fail — do not report a proof that did not happen.

(b) Delete `AND lease_owner = ?` from `renew`. Expected: `test_renew_extends_only_the_owner_s_lease` FAILS — the impostor renews. Restore.

- [ ] **Step 6: Gate and commit**

```bash
git add src/ytauto/app/scheduler/queue.py tests/unit/app/test_queue.py
git commit -m "feat: add the persistent job queue with claim-with-lease"
```

---

## Task 11: The resource governor

**Files:**
- Create: `src/ytauto/app/scheduler/governor.py`
- Test: `tests/unit/app/test_governor.py`

**Interfaces:**
- Produces: `GPU_COMPUTE_CAPACITY: int = 1`; `Governor()` with `lease(pool: str, owner: str) -> ContextManager[bool]`, `release_all(owner: str) -> int`, `available(pool: str) -> int`.

In-memory and single-process: the dispatcher owns it and workers request through it, so it needs no persistence. It is rebuilt on restart, which is correct — no leases survive a crash.

**`gpu_compute` capacity is the integer constant 1.** Never derived from `vram_mb`. `infra/gpu.detect()` reports 4096 MiB on the target machine, and deriving capacity from it invites a "4096 MiB, so 2 slots" mistake producing exactly the nondeterministic VRAM exhaustion this exists to prevent.

Only `gpu_compute` is populated. The pool abstraction takes the other three when the work that needs them exists.

- [ ] **Step 1: Write the failing tests**

```python
def test_gpu_compute_capacity_is_one() -> None:
    """A hard constant. Deriving it from vram_mb invites '4096 MiB, so 2 slots',
    which is exactly the VRAM exhaustion the governor exists to prevent."""
    assert GPU_COMPUTE_CAPACITY == 1
    assert Governor().available("gpu_compute") == 1


def test_a_lease_is_granted_and_released_by_scope(governor: Governor) -> None:
    with governor.lease("gpu_compute", "w1") as granted:
        assert granted is True
        assert governor.available("gpu_compute") == 0
    assert governor.available("gpu_compute") == 1


def test_a_second_simultaneous_gpu_lease_is_refused(governor: Governor) -> None:
    with governor.lease("gpu_compute", "w1") as first:
        assert first is True
        with governor.lease("gpu_compute", "w2") as second:
            assert second is False
    assert governor.available("gpu_compute") == 1


def test_a_refused_lease_does_not_consume_capacity(governor: Governor) -> None:
    """The refused caller must not decrement anything on the way out, or
    capacity leaks one slot per refusal."""
    with governor.lease("gpu_compute", "w1"):
        with governor.lease("gpu_compute", "w2") as second:
            assert second is False
    assert governor.available("gpu_compute") == 1


def test_a_lease_is_released_even_when_the_body_raises(governor: Governor) -> None:
    with pytest.raises(ValueError):
        with governor.lease("gpu_compute", "w1"):
            raise ValueError("stage exploded")
    assert governor.available("gpu_compute") == 1


def test_release_all_frees_a_dead_worker_s_leases(governor: Governor) -> None:
    """The reaper's hook: a worker died holding a lease and cannot release it."""
    governor.lease("gpu_compute", "w1").__enter__()
    assert governor.available("gpu_compute") == 0
    assert governor.release_all("w1") == 1
    assert governor.available("gpu_compute") == 1


def test_an_unknown_pool_is_rejected(governor: Governor) -> None:
    with pytest.raises(ValidationError, match="unknown pool"):
        with governor.lease("nonexistent", "w1"):
            pass
```

- [ ] **Step 2: Run to verify they fail**

Expected: `ModuleNotFoundError: No module named 'ytauto.app.scheduler.governor'`.

- [ ] **Step 3: Implement**

A dict of pool name to capacity, a dict of pool name to a list of holder ids, and a `@contextmanager` `lease` that appends and yields `True` when there is room, or yields `False` without appending when there is not. `release_all` removes every entry for an owner across all pools and returns the count.

Yielding `False` rather than raising is deliberate: a refused lease is a normal scheduling outcome, and the dispatcher's response is to try a different stage, not to handle an exception.

- [ ] **Step 4: Run to verify they pass**

- [ ] **Step 5: Prove the refusal path does not leak capacity**

Make the `finally` release unconditional (releasing even when the lease was refused). Expected: `test_a_refused_lease_does_not_consume_capacity` FAILS with `available == 2`, above the pool's own capacity. Restore.

- [ ] **Step 6: Gate and commit**

```bash
git add src/ytauto/app/scheduler/governor.py tests/unit/app/test_governor.py
git commit -m "feat: add the resource governor with gpu_compute capacity 1"
```

---

## Task 12: The stage runner

**Files:**
- Create: `src/ytauto/app/scheduler/runner.py`
- Test: `tests/unit/app/test_runner.py`

**Interfaces:**
- Produces: `gather_inputs(pipeline, stage_id, stage_fingerprints, artifact_store) -> Mapping[str, tuple[ArtifactRef, ...]]`; `build_spec(stage, provider_id, provider_version, inputs, settings) -> FingerprintSpec`; `run_stage(stage, ctx, cas) -> Result | Error`.

**Database-pure.** It reads no rows and writes none — inputs arrive as arguments, outputs leave as protocol messages. That is what makes it testable without a database and what keeps every write in the dispatcher.

`build_spec` flattens `inputs` into `input_digests` in **`(stage_id, artifact_name)` order**. Stating the rule here is the point: the plan previously gave none, which is how two stage authors would have picked two orders.

- [ ] **Step 1: Write the failing tests**

```python
def test_gather_inputs_collects_every_upstream_stage(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    """upstream_of is transitive, so a stage sees its grandparents' artifacts
    too, not only its direct parents'."""
    pipeline = _chain()  # fetch -> tts -> render
    fetch_ref = _put(store, "story", b"text")
    tts_ref = _put(store, "narration", b"audio")
    artifacts.record("a" * 64, "fetch", [fetch_ref])
    artifacts.record("b" * 64, "tts", [tts_ref])

    inputs = gather_inputs(
        pipeline, "render", {"fetch": "a" * 64, "tts": "b" * 64}, artifacts
    )

    assert set(inputs) == {"fetch", "tts"}
    assert [a.name for a in inputs["fetch"]] == ["story"]


def test_gather_inputs_skips_a_stage_with_no_recorded_fingerprint(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    """On a resume, an upstream stage that has not run yet contributes nothing
    rather than raising - the runner is asked for what exists."""
    pipeline = _chain()
    assert gather_inputs(pipeline, "render", {}, artifacts) == {}


def test_build_spec_orders_inputs_by_stage_then_artifact_name() -> None:
    """The flattening rule, stated once. Without it two stage authors pick two
    orders and the same stage fingerprints differently in each."""
    inputs = {
        "tts": (_ref("timings"), _ref("narration")),
        "fetch": (_ref("story"),),
    }
    spec = build_spec(_stage("render"), "ffmpeg", "7.1", inputs, {})
    assert [name for name, _ in spec.input_digests] == ["story", "narration", "timings"]


def test_build_spec_is_stable_across_dict_iteration_order() -> None:
    """Fingerprints must not depend on mapping order - a plain dict would make
    the same inputs fingerprint differently between processes."""
    a = build_spec(_stage("r"), "p", "1", {"x": (_ref("n"),), "y": (_ref("m"),)}, {})
    b = build_spec(_stage("r"), "p", "1", {"y": (_ref("m"),), "x": (_ref("n"),)}, {})
    assert compute_fingerprint(a) == compute_fingerprint(b)


def test_run_stage_returns_a_result_carrying_the_stage_s_artifacts(
    store: CasStore
) -> None:
    stage = _fake_stage("tts", produces=[("narration", b"audio")])
    message = run_stage(stage, _ctx(job_id="j1", correlation_id="c1"), store)

    assert isinstance(message, Result)
    assert [a.name for a in message.artifacts] == ["narration"]
    assert message.job_id == "j1" and message.stage_id == "tts"


def test_a_stage_raising_ProviderError_becomes_an_Error_message_with_its_kind(
    store: CasStore
) -> None:
    """The dispatcher maps kind to FATAL vs RETRYABLE; losing it here would make
    every failure look the same and a rate limit would burn the job's attempts."""
    stage = _fake_stage("tts", raises=ProviderError(
        "429", provider_id="elevenlabs", kind=ErrorKind.RATE_LIMITED, retry_after_s=30.0
    ))
    message = run_stage(stage, _ctx(job_id="j1", correlation_id="c1"), store)

    assert isinstance(message, Error)
    assert message.kind is ErrorKind.RATE_LIMITED
    assert message.retry_after_s == 30.0


def test_a_stage_raising_an_unexpected_exception_becomes_a_FATAL_Error(
    store: CasStore
) -> None:
    """An unplanned exception is not retryable - retrying a bug just burns
    attempts and delays the failure the operator needs to see."""
    stage = _fake_stage("tts", raises=ZeroDivisionError("bug"))
    message = run_stage(stage, _ctx(job_id="j1", correlation_id="c1"), store)

    assert isinstance(message, Error)
    assert message.kind is ErrorKind.FATAL
    assert "ZeroDivisionError" in message.message


def test_run_stage_writes_no_database_rows(
    store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """The runner is database-pure: that is what lets a worker call it with no
    connection, and what keeps every write in the dispatcher."""
    before = db_conn.execute("SELECT count(*) FROM cas_objects").fetchone()[0]
    run_stage(_fake_stage("tts", produces=[("narration", b"audio")]),
              _ctx(job_id="j1", correlation_id="c1"), store)
    assert db_conn.execute("SELECT count(*) FROM cas_objects").fetchone()[0] == before
```

Write `_chain()`, `_fake_stage(stage_id, *, produces=(), raises=None)` and `_ctx(...)` as module-level helpers. `_fake_stage` must increment a per-instance execution counter so later tasks can assert a stage did **not** re-run.

- [ ] **Step 2: Run to verify they fail**

- [ ] **Step 3: Implement**

`gather_inputs` walks `pipeline.upstream_of(stage_id)`, looks each stage's fingerprint up in `stage_fingerprints`, and calls `artifact_store.lookup`. A stage with no recorded fingerprint contributes nothing.

`build_spec` sorts by `(stage_id, artifact.name)` and flattens to `(name, digest)` pairs.

`run_stage` wraps the call in `try/except ProviderError` and a second `except Exception`, mapping the first to its own `kind` and the second to `ErrorKind.FATAL`. No bare `except`.

- [ ] **Step 4: Run to verify they pass**

- [ ] **Step 5: Prove the ordering rule is load-bearing**

Remove the sort in `build_spec`. Expected: `test_build_spec_is_stable_across_dict_iteration_order` FAILS with two different fingerprints. Restore. This is the same class of defect as Task 5 and deserves the same proof.

- [ ] **Step 6: Gate and commit**

```bash
git add src/ytauto/app/scheduler/runner.py tests/unit/app/test_runner.py
git commit -m "feat: add the database-pure stage runner"
```

---

## Task 13: The dispatcher and the worker entry point

**Files:**
- Create: `src/ytauto/app/scheduler/dispatcher.py`, `src/ytauto/app/worker.py`
- Test: `tests/unit/app/test_dispatcher.py`

**Interfaces:**
- Produces: `Dispatcher(conn, cas, artifacts, governor, queue)` with `tick() -> TickReport` and `run_until_idle(*, max_ticks) -> TickReport`.

The dispatcher is the **only** component that writes job state. `tick()` does one unit of work so tests can drive it deterministically instead of racing a background loop.

**The commit is one transaction**, which Task 1 makes possible:

```python
with transaction(self._conn, immediate=True):
    for staged in staged_messages:
        self._cas.record_blob(staged.digest, kind=staged.kind, size_bytes=staged.size_bytes)
        self._cas.retain(staged.digest)          # the JOB's pin, not the cache's
    self._artifacts.record(fingerprint, stage_id, artifacts)
    self._conn.execute(
        "UPDATE job_stages SET status = 'succeeded', fingerprint = ?, finished_at = ? "
        "WHERE job_id = ? AND stage_id = ?",
        (fingerprint, utc_now_iso(), job_id, stage_id),
    )
```

`retain` here is the job's in-flight pin, released when the job completes or is reaped. `ArtifactStore.record` no longer retains (Task 3), so these two are not duplicates.

- [ ] **Step 1: Write the failing tests**

```python
def test_a_cache_hit_marks_the_stage_skipped_without_spawning_a_worker(
    dispatcher: Dispatcher, spawn_spy: SpawnSpy
) -> None:
    """The single probe that delivers crash-resume, cheap iteration and
    cross-project dedup."""
    _prerecord_stage_output(dispatcher, job_id="j1", stage_id="fetch")

    report = dispatcher.tick()

    assert report.skipped == ("fetch",)
    assert spawn_spy.calls == 0, "a cache hit must not spawn a worker"


def test_a_stage_commit_is_atomic(
    dispatcher: Dispatcher, db_conn: sqlite3.Connection, store: CasStore
) -> None:
    """Blob rows, retains, the artifact record and the job_stages update land
    together or not at all."""
    digest = store.stage_file(b"audio", kind="blob")
    _fail_the_job_stages_update(dispatcher)  # monkeypatched to raise

    with pytest.raises(sqlite3.OperationalError):
        dispatcher.commit_stage("j1", "tts", "f" * 64, [_staged(digest)])

    assert db_conn.execute("SELECT count(*) FROM cas_objects").fetchone()[0] == 0
    assert db_conn.execute("SELECT count(*) FROM artifacts").fetchone()[0] == 0
    assert db_conn.execute(
        "SELECT status FROM job_stages WHERE job_id='j1' AND stage_id='tts'"
    ).fetchone()["status"] != "succeeded"


def test_a_dead_worker_releases_its_governor_leases(
    dispatcher: Dispatcher, governor: Governor
) -> None:
    """A worker cannot release what it held when it died; the reaper must."""
    governor.lease("gpu_compute", "j1:tts").__enter__()
    assert governor.available("gpu_compute") == 0

    dispatcher.reap()

    assert governor.available("gpu_compute") == 1


def test_a_dead_worker_s_job_returns_to_the_queue_at_its_last_completed_stage(
    dispatcher: Dispatcher, queue: JobQueue, db_conn: sqlite3.Connection
) -> None:
    _mark_stage(db_conn, "j1", "fetch", "succeeded")
    _mark_stage(db_conn, "j1", "tts", "running")

    dispatcher.reap()

    assert queue.claim("w2", lease_s=60) is not None, "the job must be claimable again"
    assert _status(db_conn, "j1", "fetch") == "succeeded", "completed work must survive"
    assert _status(db_conn, "j1", "tts") != "running", "the killed stage must be reset"


def test_a_RATE_LIMITED_error_defers_the_job_by_retry_after_s(
    dispatcher: Dispatcher, db_conn: sqlite3.Connection
) -> None:
    """Without available_at this could not be honoured at all - the claim query
    had no way to exclude a job that must not run until T."""
    dispatcher.handle_error(_error("j1", "tts", ErrorKind.RATE_LIMITED, retry_after_s=3600))

    row = db_conn.execute("SELECT state, available_at FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "queued"
    assert row["available_at"] > utc_now_iso()


def test_a_FATAL_error_fails_the_job_without_requeueing(
    dispatcher: Dispatcher, db_conn: sqlite3.Connection
) -> None:
    dispatcher.handle_error(_error("j1", "tts", ErrorKind.FATAL, retry_after_s=None))

    row = db_conn.execute("SELECT state, last_error FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "failed"
    assert row["last_error"]


def test_job_completion_releases_every_job_level_retain(
    dispatcher: Dispatcher, store: CasStore
) -> None:
    """After completion the outputs become LRU-evictable, which is the intended
    end state - the cache does not pin them (Task 3)."""
    digest = _complete_a_one_stage_job(dispatcher, store)
    assert store.refcount(digest) == 0
    assert digest in [d for d, _ in store.iter_evictable()]


def test_a_worker_that_staged_then_died_leaves_only_reclaimable_orphans(
    dispatcher: Dispatcher, store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """The staged/result split's new failure surface, named in the design's
    risks. A blob file with no row is exactly what sweep_orphans reclaims, and
    the single-transaction commit means no partial rows are ever written."""
    digest = store.stage_file(b"half a stage", kind="blob")
    dispatcher.reap()  # worker died before emitting result

    assert db_conn.execute("SELECT count(*) FROM cas_objects").fetchone()[0] == 0
    assert store.path_for(digest).is_file(), "the file is an orphan, not yet reclaimed"

    report = Evictor(store, EvictionPolicy(max_bytes=1)).sweep_orphans(min_age_s=0)
    assert report.orphan_blobs == 1
    assert not store.path_for(digest).is_file()
```

Write `SpawnSpy`, `_prerecord_stage_output`, `_fail_the_job_stages_update`, `_mark_stage`, `_status`, `_error`, `_staged` and `_complete_a_one_stage_job` as module-level helpers. `SpawnSpy` replaces `subprocess.Popen` so no real process is spawned in the unit tests — the real spawn is exercised in Task 14.

- [ ] **Step 2: Run to verify they fail**

- [ ] **Step 3: Implement**

`app/worker.py` reads a job/stage assignment on stdin, runs `run_stage`, writes protocol lines to stdout, exits non-zero on `Error`. It must not import Qt and must not touch SQLite — pass it the CAS root path, not a connection.

The dispatcher spawns with `subprocess.Popen`, reads stdout line by line through `decode`, ignores `None` (unknown type or version), and collects `staged` until `result` or `error`.

- [ ] **Step 4: Run to verify they pass**

- [ ] **Step 5: Prove the commit is atomic**

Remove the `with transaction(...)` wrapper so the four writes run in autocommit. Expected: `test_a_stage_commit_is_atomic` FAILS — `cas_objects` rows survive a failure at the `job_stages` update. Restore. Paste both.

- [ ] **Step 6: Add the import-linter contract for workers**

Add a `forbidden` contract proving `ytauto.app.worker` imports neither `PySide6` nor `ytauto.ui`. Then **prove the gate works**: add `import PySide6` to `worker.py` temporarily and confirm `lint-imports` fails. Phase 0's §2.3 rule — any new gate must be demonstrated failing before it is trusted — applies here.

- [ ] **Step 7: Gate and commit**

```bash
git add src/ytauto/app/scheduler/dispatcher.py src/ytauto/app/worker.py tests/unit/app/test_dispatcher.py pyproject.toml
git commit -m "feat: add the dispatcher and worker subprocess entry point"
```

---

## Task 14: The two exit criteria

**Files:**
- Test: `tests/integration/test_resume.py`

**Interfaces:**
- Consumes: everything above.

Two criteria, not one. The second exists because the whole-branch review proved the first alone is insufficient.

- [ ] **Step 1: Write the resume test**

A synthetic three-stage pipeline `fetch → tts → render`, each stage writing one small blob. Run it under the dispatcher, kill the worker subprocess **for real** (`Popen.kill()`) during stage 2, restart the dispatcher, and assert:

- stage 1 is not re-executed (assert on an execution counter the fake stage increments, not on timing)
- stage 2 completes on the resume
- the final job state is `succeeded`
- every blob the job produced is present

- [ ] **Step 2: Write the caching test — the one criterion 1 cannot cover**

```python
def test_running_the_same_job_twice_hits_the_cache_on_every_stage() -> None:
    """Criterion 1 cannot catch artifact-order drift: killed in stage 2 and
    resuming at stage 2, stage 3 was never cached, so the drift is invisible.
    Run the whole job twice and every stage must be a cache hit, with no
    downstream stage re-running."""
```

Assert every stage in the second run is `SKIPPED` and each fake stage's execution counter is still 1.

- [ ] **Step 3: Run both**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/integration/test_resume.py -v -m integration
```

- [ ] **Step 4: Prove criterion 2 catches what criterion 1 misses**

> **Corrected after execution.** This step originally said to revert Task 5's `StageResult` sort and expect the twice-run test to fail while resume passed. That does not reproduce, and the reason is architectural rather than a missing guard: `dispatcher.py` always sources a stage's inputs through `gather_inputs` → `ArtifactStore.lookup` → `ORDER BY name ASC`, so no path exists where a stage receives inputs from an in-memory `StageResult`. The fresh and cached paths are the same code path, and the ordering drift the Phase 1a review found is structurally impossible here — reverting *both* ordering sorts leaves every integration test green. Task 5's sort and Task 12's `build_spec` sort are defence in depth, worth keeping against a future dispatcher that short-circuits the re-read for performance, but neither is what criterion 2 exercises.

Disable **only** the dispatcher's cache probe — in `dispatcher.py`, replace `cached = self._artifacts.lookup(fingerprint)` with `cached = None`, leaving `gather_inputs` untouched. Expected: **the resume test still PASSES** while the twice-run test FAILS with all three stages re-executed instead of `SKIPPED`. Paste both outputs. Restore.

That contrast is the real justification for having two criteria. Criterion 1 skips a completed stage because `job_stages.status` already says SUCCEEDED — resume via job state, which never consults the cache at all. Criterion 2 runs a **second job** with its own empty `job_stages`, so every stage must skip on a fingerprint match alone. That is the mechanism behind cheap iteration and cross-project dedup, and criterion 1 cannot reach it.

Do **not** try to prove this by disabling `ArtifactStore.lookup` outright: that also breaks input gathering, so both tests fail for an unrelated reason and the contrast is destroyed.

- [ ] **Step 5: Full gate, doctor, commit**

```powershell
powershell -ExecutionPolicy Bypass -File scripts/check.ps1
.\.venv\Scripts\ytauto.exe doctor; $LASTEXITCODE
```

Expected: `ALL CHECKS PASSED`; nine `[ OK ]` rows with `database  schema v3 (head v3)`; exit 0.

```bash
git add tests/integration/test_resume.py
git commit -m "test: add the Phase 1b resume and caching exit criteria"
```

---

## Phase 1b Exit Checklist

- [ ] `scripts/check.ps1` passes: ruff, ruff format, mypy, import-linter, pytest (unit + integration)
- [ ] `ytauto doctor` green, reporting `schema v3 (head v3)`
- [ ] `import-linter` proves `core/` imports nothing internal, no layer below `ui/` imports Qt, and `app.worker` imports neither Qt nor `ui` — the last contract demonstrated failing before being trusted
- [ ] A three-stage job runs, is killed mid-flight by genuinely terminating the worker process, and resumes from its last completed stage
- [ ] The same job run twice hits the cache on every stage, with no downstream stage re-running
- [ ] `gpu_compute` never issues two simultaneous leases
- [ ] A cached artifact is evictable and the disk ceiling is enforced — the regression test for carry-forward §1.1
- [ ] `transaction(immediate=True)` inside a deferred transaction raises `TransactionError`; an inner failure rolls back only to its savepoint
- [ ] Every guard-pinning test has been demonstrated failing with its guard deleted, **and the expected failure message stated in advance**
- [ ] Every new public function carries a `Raises:` docstring section
- [ ] No `TODO` or `FIXME` on the shipped path

**Next:** Phase 2 — the first watchable video. Providers behind the Phase 1a ports, the render pipeline, and the `net_api` token bucket the governor is already shaped to take.

