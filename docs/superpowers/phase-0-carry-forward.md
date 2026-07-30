# Phase 0 → Phase 1 Carry-Forward

**Date:** 2026-07-31
**Branch:** `phase-0-foundation` (23 + 6 commits, `0eb90d9..f8dbd9a`)
**Status:** Phase 0 complete. `ytauto doctor` green, 120 unit + 3 integration tests, full gate enforcing.

This distils what Phase 0's reviews established that the code itself does not
record. Everything here is actionable input to the Phase 1 plan.

---

## 1. Traps Phase 1 will hit without deliberate action

These came out of the whole-branch review, which had all 29 commits in view and
could see cross-module problems the per-task reviews structurally could not.

### 1.1 `transaction()` needs `BEGIN IMMEDIATE` before the job queue lands

`infra/db/engine.py` issues a **deferred** `BEGIN`. Every current caller's first
statement is a write, so the write lock is taken immediately and `busy_timeout`
applies. Phase 1's claim-with-lease and governor lease acquisition are
**read-then-write**.

In WAL mode, a deferred transaction that reads and *then* tries to write while
another connection has written gets `SQLITE_BUSY_SNAPSHOT` returned
**immediately — the busy handler is never invoked**, so `busy_timeout=10000`
does nothing. This surfaces as a flaky, load-dependent queue bug that only
appears under concurrency.

**Action:** add `transaction(conn, *, immediate: bool = False)` and make the job
queue and the resource governor use `immediate=True`.

### 1.2 `transaction()` is not re-entrant, and that constrains composition

Nesting raises `OperationalError: cannot start a transaction within a
transaction`, and the *outer* transaction is rolled back as a side effect.

The consequence the existing caveat does not state: `CasStore.retain()` opens
its own transaction, so **"claim a job and retain its input assets" cannot
currently be made atomic.** That is a real Phase 1 requirement.

**Action:** either add `SAVEPOINT` support to `transaction()`, or give `CasStore`
methods an optional connection/transaction parameter so callers can compose.

### 1.3 Correlation IDs will silently break at the process boundary

`infra/logging.py` reads the correlation-ID `ContextVar` at **format** time. Once
workers report logs through a pipe and the parent re-emits them, every relayed
line gets stamped with the *parent's* correlation ID — quietly destroying the
per-job trail that is the whole point of the mechanism.

**Action:** stamp the ID onto the record at emission via a `logging.Filter`, and
carry `correlation_id` as an explicit field in the worker protocol.

### 1.4 Only the main process may install the `RotatingFileHandler`

Concurrent rollover across processes fails on Windows with `WinError 32`. The
spec's pipe-based worker logging avoids this by design, but nothing in the code
prevents a worker from calling `configure_logging` directly.

**Action:** add a `file_logging: bool` parameter (or an assertion) so a future
worker cannot casually acquire a file handler.

### 1.5 The `gpu_compute` pool size is a hard constant, not a derivation

`infra/gpu.detect()` reports `vram_mb`, but **nothing should consume it for pool
sizing.** On the 4 GB RTX 3050 the pool is capacity **1**, full stop. Deriving
capacity from `vram_mb` invites a "4096 MiB, so 2 slots" mistake that produces
exactly the nondeterministic VRAM exhaustion the governor exists to prevent.

**Action:** the Phase 1 plan must state the pool size as a constant with this
reasoning attached.

### 1.6 CAS needs an orphan sweeper — known gap, deliberately deferred

Two categories of garbage are currently unreclaimable, because `Evictor` iterates
database rows only:

- A blob whose **file** landed but whose **row** did not — file with no row.
- `{hash}.{pid}.tmp` staging files left behind by a killed worker.

The spec plans explicitly for worker death, so this leaks monotonically under
batch operation on an 84 GB disk.

**Action:** `Evictor.sweep_orphans()` walking the shard tree, plus `.tmp` files
older than N hours. Also change `forget()` to delete the row **before**
unlinking, so a crash window leaves a reclaimable orphan rather than a phantom
row that makes `total_size()` overcount.

---

## 2. Process rules that earned their place

Eight of twelve tasks needed a fix round. **Every finding traced to a defect in
the plan, not an implementer error.** Two rules follow directly.

### 2.1 Prove guards by deleting them

Phase 0 found four tests that **could not fail for the reason they existed**:

| Where | The test passed even though… |
|---|---|
| Migrations | the entire `transaction()` wrapper could be deleted |
| ffmpeg locator | `match="ffprobe"` matched *both* the success and failure messages |
| GPU detect | `assert x is None or isinstance(x, GpuInfo)` is always true |
| ffmpeg probe | the `!= "="` legend guard could be deleted |

**Rule:** when a test's purpose is to pin a *guard* — a `try/except`, a filter, a
transaction wrapper, a validation branch — the implementer must paste evidence of
the test failing **with the production guard deleted**, not merely with the
feature unimplemented.

### 2.2 The plan must specify the error contract between modules

The plan's failure mode was consistent: it specified happy paths in detail and
the **error contract between modules not at all.** Phase 0 ended with three
incompatible conventions for the same failure class — wrap into
`ConfigurationError` (`paths`), swallow to `None` (`gpu`), raise raw
(`locator`, `probe`, `engine`). `doctor` was the first real caller and guessed
wrong twice, producing a tracebacking diagnostic tool.

`Raises:` docstrings now exist across `infra/**` and `core/**`.

**Rule:** each Phase 1 task brief carries an explicit `Raises:` block alongside
its `Consumes:`/`Produces:` block, and the next task's brief consumes it. The
existing `Interfaces` convention is what caught the Task 3 discrepancy; extend it.

### 2.3 Verify gates, don't trust them

For eight tasks `scripts/check.ps1` printed `ALL CHECKS PASSED` while checking
only import-linter — `$ErrorActionPreference` does not apply to native
executable exit codes in PowerShell 5.1. A later fix attempt used
`python -m importlinter.cli lint-imports`, which exits 0 printing nothing
because that module has no `__main__` guard — a second silently-passing gate,
caught only because someone tried to make it fail.

**Rule:** any new gate must be demonstrated failing before it is trusted.

---

## 3. Known-open minor findings

Not defects today; fix alongside the next change to their module.

**Must be closed by the PR that first uses the thing:**

- `settings` table has zero behavioural coverage — columns and PK unasserted.
- The `layers` import-linter contract covers only `ui > app > core`;
  `providers`/`infra`/`cli` layering is unconstrained. Tolerable because the
  no-Qt and core-purity contracts hold **transitively** (verified adversarially),
  but revisit when `ui/` gains real content in Phase 6.

**Low-value, batch them opportunistically:**

- `parse_encoders`/`parse_filters` guard is a denylist on the separator
  character rather than a structural constraint; `([A-Za-z0-9]\S*)` would be
  strictly more robust at zero cost.
- `CasStore.size_of()` and `touch()` have no direct test coverage.
- `infra/logging.py`'s `exc_info` → `payload["exc"]` branch is never exercised —
  exception logging is the most valuable thing that formatter does.
- `locator.py`'s "directory exists but has no ffmpeg → try next candidate"
  fall-through is uncovered, so precedence is only tested at levels 1 and 2.
- `pytest-cov` is a dependency with no `--cov` config and no threshold. Either
  wire up `--cov-fail-under` (96% today; it would have flagged the doctor branch
  gaps) or drop the dependency.
- `mypy` excludes `tests/`, so `# type: ignore[arg-type]` comments there are
  never validated.
- Unreachable `parser.error` branch in `cli/__main__.py` (`required=True` means
  argparse exits first).
