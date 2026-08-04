# Phase 1a → Phase 1b Carry-Forward

**Date:** 2026-08-01
**Branch:** `phase-1a-domain-pipeline` (24 commits, `4a82dfe..34cacf4`)
**Status:** Phase 1a complete. 251 unit + 3 integration tests, full gate enforcing, `ytauto doctor` green at `schema v2 (head v2)`.

This distils what Phase 1a's reviews established that the code itself does not
record. Everything here is actionable input to the Phase 1b plan.

---

## 1. Traps Phase 1b will hit without deliberate action

These came out of the whole-branch review, which held all 24 commits in view and
could see cross-module problems the per-task reviews structurally could not.
Every one was confirmed by direct execution, and the controller independently
re-verified 1.1, 1.2 and the evictor race before ruling.

**None of them is live today.** `ArtifactStore`, `Evictor` and
`compute_fingerprint` have **zero production callers** — they are libraries with
tests and nothing else. That is the only reason this branch merges with a
Critical open. **Phase 1b is the phase that wires all three up, and it makes
every one of these live on day one.**

### 1.1 Recorded artifacts are permanently unevictable, and the evictor's only remaining prey is the running stage's output — CRITICAL

`ArtifactStore.record()` calls `CasStore.retain()` on every artifact. That `+1`
is released only by `ArtifactStore.forget()`, **which has no caller anywhere.**
`iter_evictable()` selects `WHERE refcount = 0`. So every cached artifact is
permanently outside the evictor's reach.

The compounding half is worse. As the cache fills with pinned artifacts, the
only remaining eviction candidates are blobs that have been `put_bytes`'d but
not yet `record`ed — i.e. **the outputs of stages running right now.** Measured:

```
total_size=5300  ceiling=100
evictable candidates: ['30de7cc7']      # the in-flight blob, and nothing else
-> EvictionReport(evicted=1, bytes_freed=1100, bytes_remaining=4200)
in-flight blob survived: False
```

The store sat 42× over its ceiling and the one thing the evictor deleted was a
running stage's output. The 40 GiB ceiling is decorative.

The root cause is that **two meanings are loaded onto one counter.** Phase 0's
`refcount` means "in-flight job or project asset — protect it." Phase 1a's
`record()` uses the same counter to mean "cached forever." `last_accessed_at`
and the whole LRU design exist to age cache entries out, and `record()` pins
them out of LRU's reach.

**Action — this is a design decision Phase 1b must make before writing the stage
runner, not a patch.** The recommended shape: a cache entry should **not** pin
its blobs at all. `lookup()`'s self-healing miss already exists for exactly this
— an entry whose blob was aged out becomes a miss and the stage re-runs, which
is correct cache semantics. In-flight protection then becomes the *job's*
responsibility: the job retains what it is actively using and releases on
completion or reap. Decide explicitly what releases a pin, and who owns it.

### 1.2 A stage's artifact order differs between a fresh run and a cache hit, so every downstream fingerprint drifts

`StageResult.artifacts` is a tuple in **declaration order**.
`ArtifactStore.lookup()` returns `ORDER BY name ASC`. Nothing reconciles them,
and `FingerprintSpec.input_digests` is an ordered tuple whose order is
load-bearing *by design* ("concatenating two clips the other way round produces
a different video"). Measured:

```
declaration order : ['timings', 'narration']
lookup order      : ['narration', 'timings']
downstream fp, fresh run : 844c5bc9510ebe15
downstream fp, cache path: 7a4fcd462c145f82
-> DIFFERENT
```

Any pipeline where a stage's declaration order is not alphabetical loses its
**entire downstream cache on the second run.** This is the exact hazard
`fingerprint.py`'s own module docstring names: it silently disables caching
while failing nothing.

**This is the finding that would have let Phase 1b's exit criterion pass while
the thing it demonstrates is broken.** Killed in stage 2 and resuming at stage 2,
stage 3 was never cached, so the drift is invisible. Run the same job twice and
every downstream stage re-runs.

**Action:** canonicalise artifact order at the `StageResult`/`record()` boundary
so the fresh and cached paths cannot disagree. Also put artifact **names** into
the fingerprint — today two upstream artifacts that swap names while keeping
their digests fingerprint identically, which is a false *hit*. And state the
rule for flattening `JobContext.inputs` (a `Mapping[str, tuple[...]]`) into
`input_digests`, or two stage authors will pick two different orders.

### 1.3 `lookup()` is a read that escalates to a write transaction

`ArtifactStore.lookup()` self-heals via `_drop_rows()`, which opens
`transaction(immediate=True)`. A method named `lookup` returning `tuple | None`
reads as pure; it is not. The natural stage-runner shape is fatal:

```python
with transaction(conn, immediate=True):
    conn.execute("UPDATE jobs SET state='running' …")
    store.lookup(fp)      # OperationalError, and the claim is rolled back
```

Before the fix in §1.4 this silently discarded the **caller's** transaction.

**Action:** split `lookup()` into a pure read plus an explicit `heal()`, or give
it a `heal: bool = True` parameter the scheduler can turn off inside a claim.
A cache probe must be safe to call while holding a claim.

### 1.4 `transaction()` re-entrancy — Phase 0 §1.2 is still open, with four new colliding entry points

Phase 0 asked for `SAVEPOINT` support **or** an optional connection parameter on
`CasStore`. Neither shipped; `transaction()` gained only `immediate=`. Phase 1a
then added four more transaction-opening public entry points
(`ArtifactStore.record`, `.forget`, `.lookup` via `_drop_rows`, and
`CasStore.forget_rows_without_files`).

Compositions Phase 1b needs and **cannot have**: *claim job + retain inputs*;
*mark stage succeeded + record artifacts*; *acquire GPU lease + read governor
state*; *claim + check cache*.

**Partially addressed pre-merge.** `transaction()` now raises a distinct
`TransactionError` when a transaction is already open, so nesting is a loud,
non-destructive programming error rather than a silent rollback of the outer
transaction — and a claim loop can now tell "my code is broken, crash" from
"someone else holds the lock, retry" without string-matching an error message.

**Action — the composition problem itself remains.** Phase 1b must add
`SAVEPOINT` support to `transaction()` or thread a transaction/connection
parameter through `CasStore`. Note `record()` currently performs N+1 separate
transactions (one for the INSERTs, one per `retain`), which is the direct cause
of §1.5.

### 1.5 A kill inside `record()`'s retain loop leaves an artifact unpinned, permanently and unrepairably

Distinct from the surplus `+1` documented in `_drop_rows` and deferred by
ruling — this is a **missing** `+1`, in the opposite direction, with data-loss
rather than leak consequences.

Rows commit first, then blobs are retained one transaction at a time. A kill or
a lock timeout partway through leaves rows committed and some blobs unpinned.
On resume, `lookup()` reports a **HIT** and the stage is skipped while one of its
artifacts sits at refcount 0 and inside `iter_evictable()`. Retrying `record()`
returns `False` — correct, per its idempotence contract — and never re-retains.
**No code path repairs it.** Contention alone reaches this state without any kill.

Note the asymmetry the branch's own history created: `34cacf4` widened
`forget()`'s `Raises:` to say `OperationalError` can arrive from the release loop
*after* the rows are committed. `record()`'s retain loop has the identical shape
and its `Raises:` says nothing of the kind.

**Action:** dissolves entirely if §1.4's savepoint work lands and `record()`
becomes atomic. Until then, do not treat `record()` as all-or-nothing.

### 1.6 There is no put-and-retain, so every produced artifact has an unprotected window

`put_bytes` records `refcount DEFAULT 0`, so a freshly written blob is in
`iter_evictable()` immediately. `record()`'s pre-flight checks the **file** only
(`exists()`), not the `cas_objects` row, so a blob in the orphan state passes
pre-flight and then raises from `retain()` *after* the rows are committed.

Combined with §1.1 — where the evictor's candidate set converges on exactly these
unpinned in-flight blobs — this stops being theoretical.

**Action:** provide an atomic put-and-retain, or have the job claim pin its
outputs as they are produced.

### 1.7 `Pipeline` answers a sequential runner's questions, not a scheduler's

| Scheduler question | Answerable today? |
|---|---|
| What order do stages run in? | Yes |
| Given `done`, which stages are **ready now**? | No — caller reimplements `set(depends_on) <= done` |
| Which stages can run **concurrently**? | **No** — `topological_order()` is a total order; no levels/antichains |
| Who depends on X? | Yes, but returns an unordered `frozenset` |
| What are X's transitive **ancestors**, to populate `JobContext.inputs`? | No — no `upstream_of` mirror |

The parallelism gap is the one that matters. The entire point of a governor with
`gpu_compute` capacity **1** is to run non-GPU stages alongside the one GPU
stage. `topological_order()` flattens independent stages into an arbitrary
sequence and no API reveals they were independent.

**Action:** add `ready_stages(done)`, `upstream_of(stage_id)`, and either
`levels()` or an explicit antichain accessor, before writing the governor.

### 1.8 Error-contract drift — Phase 0 §2.2 repeating in a new place

- *"The thing you asked for is not there"* raises `ValidationError` from
  `CasStore.read_bytes`/`refcount`/`size_of`, `Pipeline.stage_by_id`,
  `JobContext.input`, `StageResult.artifact` — but returns `None` from
  `ArtifactStore.lookup`.
- *"Bad input"* and *"missing state"* share `ValidationError` with **no
  discriminator**. The evidence is in the code: `ArtifactStore.forget()` needs a
  nine-line comment arguing that "the only reachable cause here is the missing
  row" because the digest was provably well-formed. A distinct type would delete
  that argument.
- Documented granularity differs: `CasStore.*` and `Evictor.*` say
  `sqlite3.Error`; `ArtifactStore.*` says `sqlite3.OperationalError`. Neither
  signals which failures are retryable.
- Nothing raises `ResourceExhausted` or `ProviderError`, and nothing uses
  `ErrorKind`, in production. The taxonomy has the right slots and infra does
  not use them.

**Phase 1b's dispatcher must map failures to `ErrorKind.FATAL` vs `RETRYABLE`
and cannot do so from `ValidationError` alone.**

**Action:** decide the retryable/fatal mapping for `infra` failures as part of
the worker protocol design, and split "missing state" from "bad input".

### 1.9 Migration 002 cannot express the retry policy Phase 1b needs

`jobs` has `attempts` but **no `available_at` / `next_attempt_at`**, so
`ProviderError.retry_after_s` and `ErrorKind.RATE_LIMITED` — which exist
precisely to defer work — cannot be honoured by the claim query, and
`idx_jobs_claimable` has no way to exclude a job that must not run until T.
`job_stages` has no per-stage attempt counter either, so one poison stage burns
the whole job's `attempts`.

**Action:** migration 003 on day one of Phase 1b. This is the situation the
plan's "the schema lands now so both build against a fixed shape" was meant to
avoid, and it did not.

### 1.10 `StageResult.meta` has nowhere to go

`StageResult.meta` is per-**result**; `artifacts.meta_json` is per-**artifact**;
`ArtifactRef` has no meta field; `record()` never writes the column. Two tasks
each defined "metadata" at a different granularity and neither connects.

**Action:** pick one granularity, or drop one of them.

---

## 2. Process rules that earned their place

Nine of ten tasks needed a fix round. **Every finding traced to a defect in the
plan, not an implementer error** — the same result as Phase 0, where it was
eight of twelve. That consistency is now the single most important fact about
how this project should be run: the plan is the defect source, and review
effort belongs there.

### 2.1 Ask implementers to report anomalies, and they will find real defects

Three times this phase an implementer was told to expect a specific failure,
saw something different, and **investigated instead of moving on**. Each time
it surfaced a genuine defect that every review had missed:

| Task | Predicted | What actually happened |
|---|---|---|
| 7 | degrading the set branch fails the iteration-order test | It did not — within one process, two sets built from the same elements iterate identically. The test could not fail for its reason. |
| 8 | removing the outer `sorted()` fails the declaration-order test | It did not — that test's fixture was a **linear chain**, which has a unique topological order regardless of tie-breaking. |
| 10 | removing the guard causes silent refcount inflation | It did not — `PRIMARY KEY (fingerprint, name)` raises `IntegrityError` first. The documented rationale was wrong in three places. |

**Rule:** every task brief must explicitly invite this — "if a predicted failure
does not materialise, or materialises for a different reason, report that rather
than smoothing it over." It is the highest-yield instruction in the brief.

### 2.2 "Confirm the test FAILS" is not a proof specification

Task 10's Step 5 said to delete a guard and confirm a named test fails. It did
fail — for an entirely unrelated reason, which is how a wrong rationale survived
into three docstrings. Any failure satisfies "confirm it fails."

**Rule:** the brief must state the expected failure **reason or message**, not
just that a failure occurs, and the implementer must report a mismatch.

### 2.3 Guard-pinning by deletion has a limit — say so when you hit it

`ArtifactStore.lookup`'s `ORDER BY name ASC` **cannot** be falsified by deletion:
`PRIMARY KEY (fingerprint, name)` creates an implicit index over exactly that
pair, so SQLite returns rows name-ordered for free — identical query plan, no
temp B-tree. Removing the clause leaves the whole suite green.

The clause was kept anyway (relying on implicit index order is precisely the
fragility it defends against) and its test was instead proven non-vacuous by
**mutation** — `ORDER BY rowid` makes it fail — with a code comment recording
why deletion cannot work here.

**Rule:** when a guard is unfalsifiable by deletion, prove non-vacuity by
mutation and record the exception **in the code**. An honest recorded exception
beats a pretend proof. This is the first exception granted to the Phase 0
guard-pinning rule and it should stay rare.

### 2.4 Reviewers are only as accurate as the constraints they are handed

One Task 8 finding was withdrawn as a controller error: the plan's constraint
reads "…or says nothing if it genuinely raises nothing," and that carve-out was
dropped when the reviewer prompt was written. The reviewer dutifully flagged a
method that provably cannot raise.

**Rule:** quote constraints **verbatim** into reviewer prompts. Never paraphrase
a constraint you wrote — paraphrase is where the carve-outs die.

### 2.5 Tests that cannot fail for their stated reason are the dominant defect class

Phase 0 shipped four. Phase 1a's reviews caught eight more before merge:

| Where | Passed even though… |
|---|---|
| Correlation-ID filter | all four tests bypassed the real dispatch path |
| Fingerprint schema version | the test never called `compute_fingerprint` at all |
| Set iteration order | it could not fail in-process for its stated reason |
| DAG inner sort | the diamond test never asserted relative order |
| DAG declaration order | its fixture was a linear chain |
| `quality_tier` bounds | boundaries pinned only negatively; `1 < tier < 5` passed everything |
| Artifact name ordering | the primary key supplied the order regardless |
| Record rollback | the failing input never opened a transaction |

**Rule:** for any test whose name asserts a *reason*, the brief must say what
mutation makes it fail. Treat "it passes" as unverified until that is shown.

---

## 3. Known-open findings

### 3.1 Fixed before merge

Two findings were cheap, prevented silent data loss, and did not depend on any
Phase 1b design decision, so they landed on this branch rather than being
carried:

- **`transaction()` nesting is now a distinct, non-destructive error** — see
  §1.4. Previously a nested `transaction()` rolled back the *outer* one.
- **`Evictor.run()` no longer deletes a blob pinned after its snapshot.** The
  evictor read `iter_evictable()` outside any transaction and then called
  `CasStore.forget()`, whose `DELETE` had no refcount predicate. A `retain()`
  landing in between did not save the blob — file and row were both destroyed
  at refcount 1, exactly the loss `_update_one`'s docstring says `retain()`
  exists to prevent. Closed with an atomic conditional delete
  (`forget_if_unreferenced`) rather than a re-check, per the Task 3 ruling that
  narrowing a race window is not closing it. `CasStore.forget()` keeps its
  unconditional contract.

### 3.2 Phase 1a minors — triaged

| Item | Verdict | Why |
|---|---|---|
| Three separate duplicate-name detectors (`Pipeline`, `StageResult`, `record()`), two of them O(n²), all with different message shapes | **fix in 1b** | Promoted from a perf nit: it is now three implementations of one rule. Consolidate into one helper. |
| `FINGERPRINT_SCHEMA_VERSION` lacks its specified `: int` annotation | **fix in 1b** | One character, `core/*` is under `mypy --strict`, and it is in the module that gates all caching. |
| No direct test of `Pipeline` equality/hashing | **fix in 1b** | Promoted by evidence: `hash(pipeline)` raises `TypeError` for a conforming-but-unhashable `Stage`. The missing test hides a real unstated precondition. |
| Dead `or hasattr(port, "capabilities")` clause in the ports annotation test | **fix in 1b** | One-line deletion; it advertises coverage the test does not have — the exact category Phase 0 §2.1 exists to stop. |
| `Echo` / `FakeStage` / two more `Fake` classes are all minimal `Stage` doubles | **fix in 1b** | 1b needs a synthetic three-stage pipeline for its exit criterion anyway. Build one shared double then. |
| Bare `str` + `type: ignore[arg-type]` passed to `path_for`; test-wide ignores now 23 | **fix in 1b** | Worsened on this branch. Fix together with "mypy excludes tests", as one change. |
| No test for a stage in `inputs` with an empty artifact tuple | **1b, deliberately** | The runner decides whether an empty tuple is legal; pin the semantics when that decision is made, not before. |
| `job_stages` column count (report said 8, it has 7) | **drop** | Report inaccuracy, no code impact. |
| `test_migrations.py` mixes `mock.patch` and `monkeypatch` | **drop** | Both correct; the file is not growing. |
| `ArtifactRef.__post_init__` discards `validate_digest`'s return | **drop** | `validate_digest` rejects rather than canonicalises, by design. |
| Literal `{"__bytes__": hex}` dict collides with the corresponding `bytes` | **drop** | Requires a caller to synthesise the internal marker; the dunder convention is the mitigation. |

Also open, found by the controller rather than a review: **four dataclass
`__post_init__` validators raise `ValidationError` with no `Raises:` section**
(`ArtifactRef`, `CapabilityDescriptor`, `Pipeline`, `StageResult`). Dataclasses
cannot carry a docstring on the generated `__init__`, so **construction has an
undocumented error contract** — and Phase 1b builds `ArtifactRef` from database
rows. Same root cause as §1.8. Relatedly, the `Stage` and provider Protocol
stubs declare no error contract at all, which is defensible under the
"genuinely raises nothing" carve-out but leaves Phase 2's providers free to
invent one each — precisely the Phase 0 failure.

### 3.3 Phase 0 carry-forward — status after Phase 1a

**Section 1 traps:** §1.1 `BEGIN IMMEDIATE` **closed**. §1.3 emission-time
correlation IDs **closed** (and on handlers, which is what makes it fire).
§1.4 `file_logging` **closed**. §1.6 orphan sweeper **closed**, with a TOCTOU
re-check and mirror reclaim beyond what was asked. **§1.2 re-entrancy remains
open** — see §1.4 above.

**Section 3 list:**

| Item | Status after Phase 1a |
|---|---|
| `settings` table has zero behavioural coverage | Open, unchanged. Still nothing uses it. |
| `layers` contract covers only `ui > app > core` | **Open, now more Important.** Phase 1a added `core.ports` (the plugin seam) and `infra.artifacts` (the core↔infra bridge) with no layering constraint across `providers`/`infra`/`cli`. Phase 1b adds the dispatcher and worker protocol in that same unconstrained region. |
| `CasStore.size_of()` / `touch()` untested | **Open, now Important.** `size_of` feeds `total_size` → the eviction ceiling, which §1.1 shows is the thing that fails. |
| `logging.py` `exc_info` → `payload["exc"]` never exercised | **Open, worsened by proximity.** Phase 1b relays worker tracebacks through this formatter. |
| `mypy` excludes `tests/` | **Open, worsened.** `# type: ignore` in `tests/` now 23, roughly half added by this branch. |
| `pytest-cov` with no `--cov` config or threshold | Open, unchanged. |
| `parse_encoders`/`parse_filters` denylist guard | Open, untouched. |
| `locator.py` fall-through uncovered | Open, untouched. |
| Unreachable `parser.error` in `cli/__main__.py` | Open, untouched. |
