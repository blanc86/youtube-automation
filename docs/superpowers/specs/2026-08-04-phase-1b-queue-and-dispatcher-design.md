# Phase 1b — Queue, Governor, Worker Protocol and Dispatcher

**Date:** 2026-08-04
**Status:** Design approved, ready for planning.
**Builds on:** `docs/superpowers/phase-1a-carry-forward.md`, `docs/superpowers/specs/2026-07-30-youtube-automation-design.md` (§3.5, §3.7, §5.3, §6.4)

Phase 1a delivered the domain and pipeline framework as libraries with no
production callers. Phase 1b is the phase that wires them up, and doing so makes
every finding in the Phase 1a carry-forward live at once. This design resolves
those findings first, then specifies the scheduler.

---

## 1. Scope

**In scope.** A persistent job queue with claim-with-lease; a resource governor
with the `gpu_compute` pool only; a versioned JSON-lines worker protocol; real
subprocess workers; a stage runner; a dispatcher that spawns, pumps and reaps.

**Out of scope, deliberately.** The `gpu_encode`, `cpu_heavy` and `net_api`
pools from §3.5. Each would be built and tested against no consumer — there is
no encoder work until the render pipeline and no provider to pace until Phase 2,
and `net_api`'s per-provider token bucket is a guess at an interface Phase 2 will
have opinions about. The governor's pool abstraction is built to take them; only
`gpu_compute` is populated.

**Success criteria.** Two, not one — see §7.

---

## 2. Decisions taken

| Question | Decision | Consequence |
|---|---|---|
| Who writes to SQLite? | **Main process only.** Workers write blob *files* and report digests over the pipe. | Dissolves the `record()` TOCTOU and the lock-contention tuning problem. Makes put-and-retain atomic. |
| What releases an artifact pin? | **Nothing — the cache does not pin.** `record()` stops retaining. | The 40 GiB ceiling becomes enforceable again. Dissolves carry-forward §1.1, §1.5, §1.6 and Task 10's deferred stranding. |
| Where is artifact order canonicalised? | **`StageResult.__post_init__` sorts by name.** | Declaration order *is* name order, so the fresh and cached paths cannot diverge. |
| Does `lookup()` still self-heal? | **Detection stays, deletion moves out.** | `lookup()` becomes a pure read that still cannot report a false hit. |
| Composition across modules? | **SAVEPOINT re-entrancy in `transaction()`.** | Closes Phase 0 carry-forward §1.2, open for two phases. |
| Migration 003? | **`jobs.available_at` + `job_stages.attempts`.** | Rate-limited retry becomes honourable; one poison stage stops burning the whole job's budget. |

### 2.1 Why the cache must not pin

Phase 1a loaded two meanings onto one counter. Phase 0's `refcount` means "an
in-flight job or a project asset depends on this — protect it." `record()` used
the same counter to mean "this is cached." Since nothing released the cache's
pin, every cached artifact became permanently unevictable, and as the cache
filled the evictor's only remaining candidates were blobs written but not yet
recorded — the outputs of *running* stages. Measured on the merged branch: with
a 100-byte ceiling and 5300 bytes stored, the single eviction candidate was the
in-flight blob, and the evictor deleted it.

A cache entry whose blob has been aged out is not a corruption; it is a miss.
`lookup()` already detects a missing blob and reports a miss, and that path is
already tested. In-flight protection moves to where it belongs: the **job**
retains what it will consume and releases on completion or reap, which the lease
already provides a hook for.

### 2.2 How savepoints interact with `TransactionError`

Phase 1a added `TransactionError` so that nesting `transaction()` became a loud,
non-destructive programming error rather than a silent rollback of the caller's
outer transaction. Making `transaction()` re-entrant via savepoints removes most
of that guard's triggers — but not all, and the remainder is a real rule:

`transaction(conn, immediate=True)` nested inside a **deferred** outer
transaction cannot deliver immediate semantics, because the write-lock timing was
already fixed by the outer `BEGIN`. Silently downgrading it would reintroduce
exactly the `SQLITE_BUSY_SNAPSHOT` failure that `immediate=` was added to
prevent. That combination keeps raising `TransactionError`.

So `transaction()` becomes:

- no open transaction → `BEGIN` / `COMMIT` / `ROLLBACK` as today
- open transaction, `immediate=False` → `SAVEPOINT` / `RELEASE` / `ROLLBACK TO`
- open transaction, `immediate=True` → `TransactionError`

---

## 3. Foundation changes to Phase 1a

These land before any scheduler code, each with its own tests.

| Module | Change |
|---|---|
| `infra/db/engine.py` | SAVEPOINT re-entrancy per §2.2. Savepoint names come from a per-connection depth counter (`_sp_0`, `_sp_1`, …) incremented on entry and decremented on exit, so sibling and nested savepoints cannot collide. |
| `infra/db/migrations.py` | Migration 003, append-only — a **new** migration that `DROP`s and re-`CREATE`s the index, never an edit to 002. Adds `jobs.available_at TEXT NOT NULL DEFAULT ''`, `job_stages.attempts INTEGER NOT NULL DEFAULT 0`, and rebuilds `idx_jobs_claimable` as `(state, available_at, priority DESC, created_at)`. The `''` default is deliberate: every timestamp in this system is ISO-8601, and `''` sorts before all of them, so existing rows are immediately claimable without a backfill. |
| `infra/artifacts.py` | `record()` no longer retains; `forget()` no longer releases; `lookup()` no longer deletes; new `heal()` reclaims stale rows. `_drop_rows`' deferred-stranding note is deleted — the condition cannot arise once nothing retains. |
| `infra/cas/store.py` | Split `put_bytes`/`put_file` into a worker-side `stage_file` (filesystem only) and a parent-side `record_blob` (row insert, composable inside a caller's transaction so put-and-retain is atomic). |
| `core/pipeline/stage.py` | `StageResult.__post_init__` sorts `artifacts` by name. |
| `core/pipeline/fingerprint.py` | Artifact **names** enter the payload alongside digests. Two artifacts swapping names while keeping digests currently fingerprint identically — a false cache *hit*. Also adds the missing `: int` on `FINGERPRINT_SCHEMA_VERSION`. Bump `FINGERPRINT_SCHEMA_VERSION`, since this invalidates every existing fingerprint. |
| `core/pipeline/graph.py` | `ready_stages(done)` and `upstream_of(stage_id)`. `topological_order()`'s docstring is corrected — it returns DFS post-order, not the lexicographically-smallest topological order. |

Consolidate the three duplicate-name detectors (`Pipeline`, `StageResult`,
`ArtifactStore.record`) into one helper with one message shape while touching
these files.

---

## 4. Modules

All under `app/scheduler/`, depending only on `core` and `infra/db` per §5.3.

**`queue.py`** — `enqueue`, `claim`, `renew_lease`, `release`, `requeue`,
`complete`, `fail`. `claim` runs in one `immediate=True` transaction:
select the highest-priority job whose `state='queued'` and
`available_at <= now`, then set `state='running'`, `lease_owner`,
`lease_expires_at`, and increment `attempts`. Read-then-write is why
`immediate=True` exists.

**`governor.py`** — a lease broker over named pools. `gpu_compute` capacity is
the integer constant **1**, never derived from `vram_mb`; deriving it invites a
"4096 MiB, so 2 slots" mistake that produces precisely the nondeterministic VRAM
exhaustion the governor exists to prevent. Leases are acquired through a context
manager and released on scope exit, on failure, and by the reaper on process
death.

**`worker_protocol.py`** — versioned JSON-lines schema, one message per line on
the worker's stdout. Every message carries `v`, `type`, `job_id`, `stage_id` and
`correlation_id` — the last as an explicit field, because Phase 0's carry-forward
§1.3 established that a relayed log line otherwise gets stamped with the
*parent's* ID.

| Type | Payload | Meaning |
|---|---|---|
| `progress` | `fraction`, `note` | advisory only, never persisted as state |
| `staged` | `digest`, `size_bytes`, `kind` | a blob file is on disk; the parent owns the row |
| `result` | `artifacts[{name, kind, digest}]`, `meta` | the stage succeeded |
| `error` | `message`, `kind`, `retry_after_s` | the stage failed; `kind` is an `ErrorKind` |
| `log` | `level`, `message`, `exc` | relayed into the parent's logger |

Unknown message types and unknown `v` are logged and ignored rather than fatal,
so a newer worker cannot wedge an older parent.

**`runner.py`** — executes one stage: gather upstream artifacts, compute the
fingerprint, probe the cache, and either mark `SKIPPED` or run the stage and emit
`staged`/`result`. Pure with respect to the database — it reads nothing and
writes nothing, taking its inputs as arguments and returning messages.

**`dispatcher.py`** — owns the loop: claim, plan, spawn, pump, reap, complete.
The only component that writes job state.

**`app/worker.py`** — subprocess entry point. Never imports Qt; `import-linter`
already proves this and the contract extends to cover it.

---

## 5. Data flow — one job, killed and resumed

**Claim.** Dispatcher claims job `J` in one immediate transaction, taking a lease.

**Plan.** Load the `Pipeline`. Read `job_stages` for `J`; the done-set is every
stage whose `StageStatus.is_done`. `ready_stages(done)` yields what may run now.

**Gather inputs.** For each upstream stage, read `job_stages.fingerprint` and
call `ArtifactStore.lookup(fp)`. Artifacts come back name-ordered, and because
`StageResult` sorted them at production time, this is the same order the fresh
run saw. Flatten to `input_digests` in `(stage_id, artifact_name)` order — stated
here so two stage authors cannot pick two orders.

**Probe.** Compute the stage fingerprint and `lookup()` it. A hit marks the stage
`SKIPPED` and moves on. That single probe is what delivers crash-resume, cheap
iteration and cross-project dedup.

**Run.** Dispatcher spawns a worker. The worker acquires a `gpu_compute` lease if
its provider's capability descriptor declares `requires_gpu`, runs the stage,
writes blob files via `stage_file`, emits `staged` per blob, then `result`.

**Commit.** On `result`, the parent does all of this in **one** transaction —
which is only possible because of savepoints:

1. `record_blob` for each staged digest
2. `retain` each one against the job's lease
3. `ArtifactStore.record(fp, stage_id, artifacts)` — which no longer retains
4. `UPDATE job_stages SET status='succeeded', fingerprint=fp, finished_at=now`

**Kill.** The worker dies mid-stage. The reaper notices via process exit or lease
expiry, releases its governor leases, releases the job-level retains for that
stage's partial outputs, resets the stage from `running`, and requeues `J` with
`available_at`.

**Resume.** `J` is claimed again. The done-set still contains the completed
stages, so `ready_stages` returns the killed one. Its upstream artifacts resolve
through `job_stages.fingerprint` exactly as before. Completed stages are not
re-run.

**Complete.** When every stage is done, `state='succeeded'` and all job-level
retains are released — at which point the outputs become LRU-evictable, which is
the intended end state.

---

## 6. Error handling

`ErrorKind` finally gets used in production, which §1.8 of the carry-forward
noted it was not.

| Failure | Response |
|---|---|
| `ProviderError` `RETRYABLE` | requeue, `available_at = now + base * 2**(attempts-1)`, capped — exponential on the job's own `attempts`, so a persistently failing job backs off instead of spinning |
| `ProviderError` `RATE_LIMITED` | requeue, `available_at = now + retry_after_s` |
| `ProviderError` `FATAL` / `QUOTA_EXCEEDED` | stage fails, job fails, `last_error` recorded |
| Worker death with no `result` | reap, release leases, requeue |
| `job_stages.attempts` exceeds the limit | that stage is fatal, job fails |
| `TransactionError` | crash. Always a programming error, never retried |
| `sqlite3.OperationalError` | retry with backoff — legitimate contention |

The distinction in the last two rows is the entire reason `TransactionError`
exists; before Phase 1a's fix both arrived as `sqlite3.OperationalError` and a
claim loop could only have told them apart by string-matching.

---

## 7. Testing and exit criteria

**Criterion 1 — resume.** A synthetic three-stage job runs, is killed mid-flight
by genuinely terminating the worker subprocess, and on restart resumes from its
last completed stage without re-running completed work.

**Criterion 2 — the cache actually caches.** The same job is run to completion
**twice**, and the second run is asserted to hit the cache on *every* stage, with
no downstream stage re-running.

Criterion 2 exists because the whole-branch review proved criterion 1 alone is
not enough. Killed in stage 2 and resuming at stage 2, stage 3 was never cached,
so the artifact-ordering drift is invisible — criterion 1 would have passed while
the caching it demonstrates was broken. A criterion that passes without
exercising the thing it exists to test is the dominant defect class in this
project, and this is the deliberate guard against repeating it.

**Additional required proofs.**

- Two workers claiming concurrently: exactly one wins; the loser gets no lease.
- A lease that expires is reaped and its job becomes claimable again.
- `gpu_compute` never issues a second simultaneous lease.
- Savepoint rollback: an inner failure rolls back only to the savepoint, and the
  outer transaction can still commit.
- `transaction(immediate=True)` inside a deferred transaction raises
  `TransactionError`.
- With `record()` no longer retaining, a cached artifact **is** evictable and the
  ceiling is enforced — the direct regression test for carry-forward §1.1.

Per the Phase 1a process rules, every guard-pinning test states the mutation that
makes it fail, and any predicted failure that does not materialise — or
materialises for a different reason — is reported rather than smoothed over.

---

## 8. Risks

**The `staged`/`result` split is a new failure surface.** A worker can emit
`staged` and die before `result`, leaving blob files with no rows. This is the
orphan state `Evictor.sweep_orphans` already reclaims, and the parent's
single-transaction commit means no partial rows are ever written. Named here so
it is tested, not discovered.

**Bumping `FINGERPRINT_SCHEMA_VERSION` invalidates every cached artifact.** That
is correct and intended — the old fingerprints were computed without artifact
names and are not trustworthy — but it means the first run after this change
recomputes everything.
