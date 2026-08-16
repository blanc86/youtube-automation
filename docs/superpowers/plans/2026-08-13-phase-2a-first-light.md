# Phase 2a "First Light" Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A pasted story becomes two rendered, narrated, word-captioned videos — landscape and vertical — driven from the CLI.

**Architecture:** Seven stages on the existing content-addressed DAG. The dispatcher spawns one worker per stage; workers never touch SQLite. Providers sit behind the existing ports and are resolved through entry points so `app/` never imports `providers/`. Two sibling compose stages render the two canvases from identical upstream artifacts.

**Tech Stack:** Python 3.12, SQLite (WAL), ffmpeg + `h264_nvenc`, `edge-tts`, libass, pytest, mypy, ruff, import-linter.

**mypy strictness, stated precisely** — `pyproject.toml` sets `packages = ["ytauto"]` with `strict = true` **only** under the `ytauto.core.*` override. Everything outside `core` is checked at mypy's default settings, and `tests/` is not checked at all. So: code you add under `src/ytauto/core/` must satisfy `--strict`; code under `app/`, `infra/`, `providers/` and `cli/` must satisfy the default profile. Annotate new code fully regardless — but do not report a task as blocked because non-`core` code would fail `--strict`, and do not tell a reviewer the whole tree is strict.

**Spec:** `docs/superpowers/specs/2026-08-13-phase-2a-first-light-design.md`

## Global Constraints

- **The gate is `python scripts/check.py`.** It must pass before every commit. It runs ruff check, ruff format, mypy, import-linter, unit tests, integration tests.
- **`ytauto.core` may import nothing internal.** Not `infra`, `app`, `providers`, `ui`, `cli`. Enforced by a `forbidden` contract.
- **`ytauto.app` may not import `ytauto.providers`.** Enforced by the contract *"app depends only on core and infra"*. This is why the registry uses entry points (Task 3).
- **Workers must never execute a SQL statement.** Only `CasStore.stage_file`, `.exists`, `.path_for`, `.read_bytes` are filesystem-only and worker-safe. `put_bytes`, `record_blob`, `retain`, `release`, `touch`, `read` of `cas_objects` are parent-only.
- **`ResourceWarning` and `PytestUnraisableExceptionWarning` are errors.** Every file handle and pipe must be explicitly closed. A leaked handle fails the suite.
- **Never put a filesystem path into a fingerprint.** `JobContext.workdir` must not reach `FingerprintSpec.settings`.
- **`StageResult.artifacts` is auto-sorted by name.** Do not rely on declaration order; encode ordering in names (`seg_000`, `seg_001`).
- **Guard-pinning is mandatory.** For any test whose name asserts a *reason*, delete the guard and confirm that test fails **for that reason**. **If a predicted failure does not materialise, or materialises for a different reason, report that — do not smooth it over.** Where a guard cannot be falsified by deletion, prove non-vacuity by mutation and record the exception in a code comment.
- **Pin the public entry point, not just the helper it delegates to.** Learned in Task 5 at the cost of a fix round: its brief pinned `_consume`'s error mapping, but the real defect sat at a seam the test could not cross — `edge_tts.Communicate.__init__` validates synchronously and raises *before* `_consume` runs at all, so the `except` branch was dead code for its own headline case while the test passed happily. A test that cannot reach the production path gives confidence it has not earned.
- **Every method named in a task's Produces interface needs a test**, whether or not the task's Step text spells one out. Learned in Task 2 at the cost of a fix round: `set_setting` shipped with a documented partial-update contract and zero coverage, because Step 5 listed two tests and the implementer wrote exactly two. If a Step's test list is shorter than the Produces list, the Step is incomplete — say so and cover the gap.

## Rigour Dial

Per requirements §9, review effort is not uniform. Tasks **1, 3, 7, 8, 11, 12** touch the render path, the cache, or fingerprints — a bug there is expensive and silent, so they get full per-task review. Tasks **4, 5, 6, 10** are straightforward provider wrappers; write the tests, run the gate, move on.

---

## File Structure

**Created:**

| Path | Responsibility |
|---|---|
| `src/ytauto/core/models/narration.py` | `WordBoundary`, `Narration` — the widened TTS/transcribe payload |
| `src/ytauto/core/pipeline/timeline.py` | `CaptionGroup`, `Segment`, `Timeline`, `plan_timeline` — pure |
| `src/ytauto/core/captions/ass.py` | `render_ass` — pure `.ass` writer, canvas-parameterised |
| `src/ytauto/app/stage_support.py` | `project_settings`, `stage_fingerprint` — the one fingerprint helper |
| `src/ytauto/app/registry.py` | Entry-point resolution: `build_stage`, `build_pipeline` |
| `src/ytauto/app/stages/*.py` | The seven `Stage` classes |
| `src/ytauto/providers/story/pasted.py` | `PastedStorySource` |
| `src/ytauto/providers/tts/edge.py` | `EdgeTtsSynthesizer` |
| `src/ytauto/providers/transcribe/edge_boundary.py` | `EdgeBoundaryTranscriber` |
| `src/ytauto/providers/visual/library.py` | `LibraryVisualStrategy` |
| `src/ytauto/infra/broll.py` | Clip probing, dual normalisation, manifest writing |
| `src/ytauto/infra/ffmpeg/compose.py` | ffmpeg argument construction — pure, returns `list[str]` |
| `src/ytauto/app/services/projects.py` | Project row CRUD, settings load |

**Modified:** `infra/db/migrations.py` (004), `app/scheduler/dispatcher.py` (stderr, deadline, settings, registry), `app/worker.py` (registry), `core/ports/providers.py` (port widening), `cli/__main__.py` (commands), `pyproject.toml` (entry points, `edge-tts`).

---

## Task 1: Kill the stderr deadlock and bound the pump

The measured blocker: 60 KB of worker stderr deadlocks `_pump` permanently, and `proc.wait(timeout=30)` never fires because it sits *after* a stdout loop that never reaches EOF. ffmpeg clears 60 KB in seconds. **No stage that shells out to ffmpeg can exist until this lands.**

**Files:**
- Modify: `src/ytauto/app/scheduler/dispatcher.py:722-899` (`_spawn`, `_pump`)
- Test: `tests/unit/app/test_dispatcher.py`, `tests/integration/test_worker_stderr.py` (create)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `Dispatcher.__init__` gains `pump_deadline_s: float = 1800.0`. `_spawn` writes worker stderr to `ctx.workdir / f"stderr.attempt-{attempts}.log"`.

- [ ] **Step 1: Write the failing integration test for the flood**

Create `tests/integration/test_worker_stderr.py`:

```python
import pytest

pytestmark = pytest.mark.integration


def test_a_worker_that_floods_stderr_still_reports_its_result(dispatcher_env):
    """200 KB of stderr must not deadlock the pump. 60 KB was measured as fatal."""
    # The bounded deadline is load-bearing for Step 9's guard-pin, not just
    # for this run: with the default 1800 s, reverting the stderr fix would
    # hang for half an hour instead of failing, and the predicted failure
    # could not be observed.
    env = dispatcher_env(stage="tests.integration.stages:StderrFlooder", pump_deadline_s=60.0)
    report = env.dispatcher.tick()
    assert report.spawned == ("flood",), "the flooding stage must complete, not hang"
    assert env.stage_status("flood") == "succeeded"
```

Add the flooding stage to `tests/integration/stages.py`:

```python
class StderrFlooder:
    """Writes 200 KB to stderr, then produces one artifact."""

    id = "flood"
    version = 1
    depends_on: tuple[str, ...] = ()
    settings_keys: tuple[str, ...] = ()

    def __init__(self, cas, settings):
        self._cas = cas

    def fingerprint(self, ctx):
        return "f" * 64

    def run(self, ctx, emit):
        import sys

        sys.stderr.write("x" * 200_000)
        sys.stderr.flush()
        digest = self._cas.stage_file(b"done", kind="text")
        return StageResult(artifacts=(ArtifactRef(name="out", kind="text", digest=digest),))
```

- [ ] **Step 2: Run it and confirm it hangs**

Run: `pytest tests/integration/test_worker_stderr.py -v -m integration --timeout=60`

Expected: **the test hangs and is killed at 60 s**, not an assertion failure. That hang *is* the bug. Record the observation. (Add `pytest-timeout` to the dev extra in this step — without it the suite hangs forever rather than failing.)

- [ ] **Step 3: Redirect stderr to a per-attempt log file**

In `_spawn`, replace `stderr=subprocess.PIPE`. Before `Popen`:

```python
log_path = ctx.workdir / f"stderr.attempt-{claimed.attempts}.log"
log_path.parent.mkdir(parents=True, exist_ok=True)
stderr_file = log_path.open("wb")
```

Pass `stderr=stderr_file` to `Popen`, and pass `stderr_file` through to `_pump`, which closes it in its `finally`. On the `not started` path in `_spawn`'s `finally`, close it there instead — it must be closed on **every** path or the `ResourceWarning` gate fails.

Delete the two `proc.stderr` lines from `_pump`'s `finally`; there is no stderr pipe any more.

- [ ] **Step 4: Run the flood test again**

Run: `pytest tests/integration/test_worker_stderr.py -v -m integration`
Expected: PASS. Confirm the log file exists and is 200,000 bytes. Note the filename is `stderr.attempt-1.log`, not `-0`: `JobQueue.claim()` increments `attempts` before returning the `ClaimedJob`, so the first attempt is 1.

- [ ] **Step 5: Write the failing deadline test**

The flood fix does not bound a worker that writes nothing and never exits. In `tests/unit/app/test_dispatcher.py`:

```python
def test_a_worker_that_never_exits_is_killed_by_the_pump_deadline(db_conn, tmp_path):
    """A silent, immortal worker must not hang the dispatcher forever."""
    dispatcher = _dispatcher(db_conn, tmp_path, pump_deadline_s=0.2)
    _enqueue(db_conn, "j1", stages=("fetch",))

    report = dispatcher.tick()

    assert report.spawned == ("fetch",)
    assert _stage_status(db_conn, "j1", "fetch") == "pending", "a timed-out stage must reset"
    assert _stage_attempts(db_conn, "j1", "fetch") == 1, "a deadline kill must charge an attempt"
    assert "deadline" in _job_last_error(db_conn, "j1")
```

Drive it with a spawn double whose stdout never closes and whose process never exits.

- [ ] **Step 6: Run it and confirm it hangs**

Run: `pytest tests/unit/app/test_dispatcher.py::test_a_worker_that_never_exits_is_killed_by_the_pump_deadline -v --timeout=30`
Expected: hangs, killed at 30 s. Same observation as Step 2, different cause.

- [ ] **Step 7: Add the watchdog**

A reader thread plus a queue would also work, but a watchdog is a far smaller change and leaves the read loop untouched: killing the process closes stdout, which ends the existing loop naturally.

```python
import threading

# in _pump, before the try:
timed_out = False

def _kill_on_deadline() -> None:
    nonlocal timed_out
    timed_out = True
    proc.kill()

watchdog = threading.Timer(self._pump_deadline_s, _kill_on_deadline)
watchdog.start()
```

`watchdog.cancel()` goes first in the `finally`. In the no-terminal-message branch, make the message honest:

```python
reason = (
    f"exceeded the {self._pump_deadline_s}s pump deadline and was killed"
    if timed_out
    else f"exited without a terminal message (exit code {proc.returncode})"
)
```

- [ ] **Step 8: Run both tests plus the full gate**

Run: `python scripts/check.py`
Expected: ALL CHECKS PASSED, unit count +1, integration count +1.

- [ ] **Step 9: Guard-pin both fixes**

Revert `stderr=stderr_file` to `stderr=subprocess.PIPE`, keeping the watchdog. Run the flood test.

**Predicted:** it does not hang — the 60 s watchdog kills the deadlocked worker — so the test fails roughly 60 s in, on `assert env.stage_status("flood") == "succeeded"` receiving `'pending'`.

**Not** on `assert report.spawned == ("flood",)`: `_spawn` returns `True` and `tick()` reports the spawn as soon as `Popen` succeeds and the assignment is written, so reverting the stderr fix cannot make that assertion false. Only the stage's *outcome* changes.

Expect a second failure too: a teardown `PytestUnraisableExceptionWarning` for the leaked stderr pipe, promoted to an error by `filterwarnings`. Step 3 deletes the `proc.stderr.close()` lines, so reverting only the `Popen` argument leaves the pipe with nothing to close it. That is the historical leaked-pipe gate firing correctly — independent confirmation the fix matters.

A hang means the watchdog is not firing and Step 7 is broken too.

Then restore, and delete `watchdog.start()`. Run the deadline test.
**Predicted:** hangs, killed by `--timeout`.

Report both. If either behaves differently, say so rather than adjusting the test.

- [ ] **Step 10: Commit**

```bash
git add src/ytauto/app/scheduler/dispatcher.py tests/ pyproject.toml
git commit -m "fix: send worker stderr to a file and bound the pump with a deadline"
```

---

## Task 2: Migration 004 and project persistence

**Files:**
- Modify: `src/ytauto/infra/db/migrations.py`
- Create: `src/ytauto/app/services/projects.py`
- Test: `tests/unit/infra/test_migrations.py`, `tests/unit/app/test_projects.py`

**Interfaces:**
- Produces: `HEAD_VERSION == 4`. `ProjectService(conn).create(slug, title, story_digest, settings) -> str` (returns project id); `.get(project_id) -> ProjectRow`; `.settings_for(project_id) -> dict[str, object]`; `.set_setting(project_id, key, value) -> None`.

- [ ] **Step 1: Write the failing migration test**

```python
def test_migration_004_adds_projects_and_broll_clips(db_conn):
    apply_migrations(db_conn)
    assert current_version(db_conn) == 4
    tables = {r["name"] for r in db_conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"projects", "broll_clips"} <= tables


def test_broll_clips_records_a_licence_and_both_normalised_digests(db_conn):
    """Provenance is not optional - it is the DMCA defence for the channel."""
    apply_migrations(db_conn)
    cols = {r["name"] for r in db_conn.execute("PRAGMA table_info(broll_clips)")}
    assert {"source_url", "licence", "attribution", "notes"} <= cols
    assert {"normalised_landscape_digest", "normalised_vertical_digest"} <= cols
```

- [ ] **Step 2: Run and confirm it fails**

Run: `pytest tests/unit/infra/test_migrations.py -v -k 004`
Expected: FAIL, `assert 3 == 4`.

- [ ] **Step 3: Add `_M004`**

```python
_M004 = Migration(
    version=4,
    name="projects_and_broll",
    statements=(
        """
        CREATE TABLE projects (
            id            TEXT PRIMARY KEY,
            slug          TEXT NOT NULL UNIQUE,
            title         TEXT NOT NULL,
            story_digest  TEXT,
            settings_json TEXT NOT NULL DEFAULT '{}',
            created_at    TEXT NOT NULL,
            updated_at    TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE broll_clips (
            id                          TEXT PRIMARY KEY,
            source_digest               TEXT NOT NULL,
            normalised_landscape_digest TEXT NOT NULL,
            normalised_vertical_digest  TEXT NOT NULL,
            duration_s                  REAL NOT NULL,
            width                       INTEGER NOT NULL,
            height                      INTEGER NOT NULL,
            source_url                  TEXT NOT NULL,
            licence                     TEXT NOT NULL,
            attribution                 TEXT NOT NULL DEFAULT '',
            notes                       TEXT NOT NULL DEFAULT '',
            added_at                    TEXT NOT NULL
        )
        """,
        "CREATE INDEX idx_broll_source_digest ON broll_clips (source_digest)",
    ),
)

MIGRATIONS: tuple[Migration, ...] = (_M001, _M002, _M003, _M004)
```

- [ ] **Step 4: Run and confirm both tests pass**

Run: `pytest tests/unit/infra/test_migrations.py -v`

- [ ] **Step 5: Write the failing `ProjectService` test**

```python
def test_settings_round_trip_through_json(db_conn):
    svc = ProjectService(db_conn)
    pid = svc.create(slug="ghost-train", title="Ghost Train", story_digest=None,
                     settings={"voice": "en-US-GuyNeural", "seed": 7})
    assert svc.settings_for(pid) == {"voice": "en-US-GuyNeural", "seed": 7}


def test_a_duplicate_slug_is_refused(db_conn):
    svc = ProjectService(db_conn)
    svc.create(slug="dup", title="A", story_digest=None, settings={})
    with pytest.raises(ValidationError, match="slug"):
        svc.create(slug="dup", title="B", story_digest=None, settings={})
```

- [ ] **Step 6: Implement `ProjectService`**

Wrap writes in `transaction(conn, immediate=True)`. Catch `sqlite3.IntegrityError` on the UNIQUE slug and re-raise as `ValidationError` naming the slug — the dispatcher's error mapping needs "bad input" distinguishable from "missing state" (carry-forward §1.8).

- [ ] **Step 7: Run the gate and commit**

```bash
python scripts/check.py
git add src/ytauto/infra/db/migrations.py src/ytauto/app/services/projects.py tests/
git commit -m "feat: add migration 004 for projects and the B-roll library"
```

---

## Task 3: The seam — port widening, settings, registry, fingerprint helper

These are one task because they are one seam: no stage can exist until all four land. **This is the highest-review task in the plan.** A wrong settings projection disables caching while failing nothing.

**Files:**
- Create: `src/ytauto/core/models/narration.py`, `src/ytauto/app/stage_support.py`, `src/ytauto/app/registry.py`
- Modify: `src/ytauto/core/ports/providers.py`, `src/ytauto/app/scheduler/dispatcher.py`, `src/ytauto/app/worker.py`, `pyproject.toml`
- Test: `tests/unit/core/test_narration.py`, `tests/unit/app/test_stage_support.py`, `tests/unit/app/test_registry.py`

**Interfaces:**
- Consumes: `ProjectService.settings_for` (Task 2).
- Produces:
  - `WordBoundary(text: str, start_s: float, duration_s: float)`, `Narration(audio: bytes, boundaries: tuple[WordBoundary, ...] | None)`
  - `SpeechSynthesizer.synthesize(text: str, *, voice: str) -> Narration`
  - `Transcriber.transcribe(narration: Narration) -> tuple[tuple[str, float, float], ...]`
  - `project_settings(settings: Mapping[str, object], keys: tuple[str, ...]) -> dict[str, object]`
  - `stage_fingerprint(stage, ctx, *, provider_id: str, provider_version: str) -> str`
  - `build_stage(pipeline_id: str, stage_id: str, cas: CasStore, settings: Mapping[str, object]) -> Stage`
  - `build_pipeline(pipeline_id: str, cas: CasStore, settings: Mapping[str, object]) -> Pipeline`
  - Every `Stage` implementation gains `settings_keys: tuple[str, ...]`.

- [ ] **Step 1: Write the failing settings-projection test**

This is the test that pins criterion 4. In `tests/unit/app/test_stage_support.py`:

```python
def test_a_stage_fingerprint_ignores_settings_it_did_not_declare():
    """Changing the caption colour must not re-run edge-tts."""
    stage = _FakeStage(id="synthesize_speech", version=1, settings_keys=("voice",))
    ctx_a = _ctx(settings={"voice": "en-US-GuyNeural", "caption_colour": "#ff0000"})
    ctx_b = _ctx(settings={"voice": "en-US-GuyNeural", "caption_colour": "#00ff00"})

    fp_a = stage_fingerprint(stage, ctx_a, provider_id="edge-tts", provider_version="1")
    fp_b = stage_fingerprint(stage, ctx_b, provider_id="edge-tts", provider_version="1")

    assert fp_a == fp_b, "an undeclared setting must not enter the fingerprint"


def test_a_stage_fingerprint_changes_when_a_declared_setting_changes():
    """The non-vacuous contrast: without this, returning a constant would pass above."""
    stage = _FakeStage(id="synthesize_speech", version=1, settings_keys=("voice",))
    fp_a = stage_fingerprint(stage, _ctx(settings={"voice": "en-US-GuyNeural"}),
                             provider_id="edge-tts", provider_version="1")
    fp_b = stage_fingerprint(stage, _ctx(settings={"voice": "en-GB-RyanNeural"}),
                             provider_id="edge-tts", provider_version="1")
    assert fp_a != fp_b, "a declared setting must enter the fingerprint"


def test_the_workdir_never_reaches_a_fingerprint():
    stage = _FakeStage(id="s", version=1, settings_keys=("voice",))
    fp_a = stage_fingerprint(stage, _ctx(workdir=Path("/tmp/a"), settings={"voice": "v"}),
                             provider_id="p", provider_version="1")
    fp_b = stage_fingerprint(stage, _ctx(workdir=Path("/tmp/b"), settings={"voice": "v"}),
                             provider_id="p", provider_version="1")
    assert fp_a == fp_b
```

- [ ] **Step 2: Run and confirm they fail**

Run: `pytest tests/unit/app/test_stage_support.py -v`
Expected: FAIL, `ModuleNotFoundError: ytauto.app.stage_support`.

- [ ] **Step 3: Implement `stage_support.py`**

```python
def project_settings(settings: Mapping[str, object], keys: tuple[str, ...]) -> dict[str, object]:
    """Narrow settings to the keys a stage declared.

    Load-bearing: FingerprintSpec.settings is hashed whole, so passing the
    full project settings would make every stage's fingerprint depend on
    every setting - changing a caption colour would re-run edge-tts. A key
    that is absent is simply omitted, so adding an unrelated setting to a
    project never invalidates a stage that does not read it.
    """
    return {key: settings[key] for key in keys if key in settings}


def stage_fingerprint(
    stage: Stage, ctx: JobContext, *, provider_id: str, provider_version: str
) -> str:
    spec = build_spec(
        stage,
        provider_id,
        provider_version,
        ctx.inputs,
        project_settings(ctx.settings, stage.settings_keys),
    )
    return compute_fingerprint(spec)
```

- [ ] **Step 4: Run and confirm all three pass**

Run: `pytest tests/unit/app/test_stage_support.py -v`

- [ ] **Step 5: Widen the ports**

Create `core/models/narration.py`:

```python
@dataclass(frozen=True)
class WordBoundary:
    """One word's span, as reported by a TTS engine that emits boundaries.

    Raises:
        ValidationError: if text is empty or duration_s is negative.
    """

    text: str
    start_s: float
    duration_s: float

    def __post_init__(self) -> None:
        if not self.text:
            raise ValidationError("WordBoundary.text must not be empty")
        if self.duration_s < 0:
            raise ValidationError(f"WordBoundary.duration_s must not be negative: {self.duration_s}")


@dataclass(frozen=True)
class Narration:
    """Synthesised speech, plus word boundaries when the engine emits them.

    ``boundaries`` is None for engines that produce audio only (Piper,
    ElevenLabs). That is precisely the case that forces ASR, so
    EdgeBoundaryTranscriber refuses it loudly rather than fabricating timings.
    """

    audio: bytes
    boundaries: tuple[WordBoundary, ...] | None
```

In `core/ports/providers.py`, change the two signatures and their docstrings. Add `settings_keys` to the `Stage` protocol in `core/pipeline/stage.py`.

- [ ] **Step 6: Write the failing registry test**

```python
def test_build_stage_resolves_through_entry_points(cas, monkeypatch):
    stage = build_stage("story_video", "ingest_story", cas, {"story_path": "x.txt"})
    assert stage.id == "ingest_story"


def test_an_unknown_stage_id_names_what_was_available(cas):
    with pytest.raises(ValidationError, match="ingest_stroy"):
        build_stage("story_video", "ingest_stroy", cas, {})
```

- [ ] **Step 7: Implement the registry with entry points**

```python
_GROUP = "ytauto.stages"


def build_stage(
    pipeline_id: str, stage_id: str, cas: CasStore, settings: Mapping[str, object]
) -> Stage:
    """Construct one stage by entry-point name.

    Resolution is dynamic on purpose. `pyproject.toml` declares a forbidden
    contract - "app depends only on core and infra" - so importing a concrete
    provider here would break the gate. This is also what
    core/ports/providers.py already specifies: a new engine is added by
    registering an entry point, with no change to core/ or app/.

    Raises:
        ValidationError: no entry point is registered under this name.
    """
    name = f"{pipeline_id}:{stage_id}"
    found = {ep.name: ep for ep in entry_points(group=_GROUP)}
    try:
        factory = found[name].load()
    except KeyError:
        raise ValidationError(
            f"no stage registered as {name!r}; registered: {sorted(found)}"
        ) from None
    stage: Stage = factory(cas=cas, settings=settings)
    return stage
```

Declare the seven entry points in `pyproject.toml` under `[project.entry-points."ytauto.stages"]` as they are built (Tasks 4–12). Add the two that exist after this task only.

- [ ] **Step 8: Cover `project_settings` directly**

`stage_fingerprint`'s tests exercise the projection only indirectly. These pin it on its own:

```python
def test_projecting_omits_keys_the_settings_do_not_have():
    """An absent declared key must be omitted, not defaulted to None -
    a None would enter the hash and differ from the key being absent."""
    assert project_settings({"voice": "v"}, ("voice", "rate")) == {"voice": "v"}


def test_projecting_an_empty_key_tuple_yields_an_empty_mapping():
    """A stage that declares no settings must fingerprint identically
    regardless of what the project settings contain."""
    assert project_settings({"voice": "v", "seed": 3}, ()) == {}
```

- [ ] **Step 9: Wire settings and the registry into the dispatcher and worker**

In `dispatcher.tick()`, replace `settings={}` with `self._projects.settings_for(claimed.project_id)`. In `_build_assignment`, replace `stage_import` with `pipeline_id`. In `worker.main()`, replace `_load_stage(assignment["stage_import"])` with `build_stage(assignment["pipeline_id"], assignment["stage_id"], cas, assignment["settings"])` and delete `_load_stage`.

**Migrate `tests/integration/stages.py` in this step.** Task 1 added test stages that are zero-arg constructed, because that is what `_load_stage` required. Replacing `_load_stage` with a registry factory breaks them, and the integration suite with them. They must move to the `factory(cas=..., settings=...)` shape in the same commit.

- [ ] **Step 10: Pin the settings plumbing end to end**

The projection tests prove the *helper* is right. Nothing yet proves real project settings actually reach a stage — which is the entire point of §4.2, and the thing `settings={}` silently prevented.

```python
def test_a_stages_context_carries_the_projects_real_settings(db_conn, tmp_path):
    """The dispatcher hardcoded settings={} until this task. A regression to
    that would leave every stage running on defaults, failing nothing."""
    ProjectService(db_conn).create(slug="s", title="T", story_digest=None,
                                   settings={"voice": "en-GB-RyanNeural"})
    seen: dict[str, object] = {}
    dispatcher = _dispatcher(db_conn, tmp_path, capture_ctx=seen.update)
    _enqueue_for_project(db_conn, "j1", slug="s")

    dispatcher.tick()

    assert seen["voice"] == "en-GB-RyanNeural", "project settings must reach the JobContext"


def test_the_assignment_carries_pipeline_id_not_a_stage_import(db_conn, tmp_path):
    """The worker resolves stages through the registry now; a lingering
    stage_import would work by reflection and silently bypass the registry."""
    assignment = _build_assignment(_claimed(), _stage(), _ctx(), "f" * 64, "/cas")
    assert assignment["pipeline_id"] == "story_video"
    assert "stage_import" not in assignment
```

- [ ] **Step 11: Run the gate**

Run: `python scripts/check.py`
Expected: ALL CHECKS PASSED, and `import-linter` still reports **4 kept, 0 broken**. If "app depends only on core and infra" breaks, the registry is importing a provider statically — fix the registry, not the contract.

- [ ] **Step 12: Guard-pin the projection**

Change `project_settings` to `return dict(settings)`.
**Predicted:** `test_a_stage_fingerprint_ignores_settings_it_did_not_declare` fails with two differing hex digests — *not* an error, and *not* the contrast test. Confirm the contrast test still passes, which is what proves the first test is not vacuous.

- [ ] **Step 13: Guard-pin the settings plumbing**

Revert `dispatcher.tick()` to `settings={}`.
**Predicted:** `test_a_stages_context_carries_the_projects_real_settings` fails with a `KeyError: 'voice'` on `seen["voice"]` — the context carries an empty mapping, so the key is absent rather than wrong. If it fails some other way, report that.

- [ ] **Step 14: Commit**

```bash
git add -A
git commit -m "feat: widen the TTS ports, plumb settings, and resolve stages via entry points"
```

---

## Task 4: `ingest_story` and `PastedStorySource`

Light review — a straightforward wrapper.

**Files:**
- Create: `src/ytauto/providers/story/pasted.py`, `src/ytauto/app/stages/ingest_story.py`
- Test: `tests/unit/providers/test_pasted_story.py`, `tests/unit/app/stages/test_ingest_story.py`

**Interfaces:**
- Consumes: `stage_fingerprint`, `build_stage` (Task 3).
- Produces: stage id `ingest_story`, `settings_keys = ("story_digest",)`, emits one artifact `story.txt` of kind `text`.

The stage reads `settings["story_path"]` at run time but fingerprints over `settings["story_digest"]` — the digest is computed by the CLI at enqueue time. Fingerprinting the path would put a filesystem path into the hash; fingerprinting by reading the file would make `fingerprint()` impure.

### Three things Task 3 established that bind this task

**1. This is the first task that registers an entry point, so it is the first that must reinstall.** Task 3 built the registry but registered nothing, because no stage existed yet. After adding to `[project.entry-points."ytauto.stages"]` you **must** run:

```bash
pip install -e ".[dev]"
```

Forgetting produces an entry point that silently does not exist — `build_stage` raises `ValidationError: no stage registered as 'story_video:ingest_story'; registered: [...]`. The name format is `"<pipeline_id>:<stage_id>"`; setuptools preserves the colon (verified in Task 3).

**2. Add the id-vs-name guard to `build_stage` as part of this task.** It was a deferred Minor from Task 3's review, promoted here because this task creates the first of seven such entries:

```python
if stage.id != stage_id:
    raise ValidationError(
        f"entry point {name!r} constructed a stage whose id is {stage.id!r}"
    )
```

Without it, a mismatched entry point builds a valid `Pipeline` and a valid fingerprint dispatcher-side, and then **every worker crashes** with "no stage registered as …" until the job exhausts `_MAX_STAGE_ATTEMPTS`. Loud, but in the wrong process at the wrong time. Pin it with a test.

**3. The fingerprint-divergence hazard applies to every factory from here on.** The dispatcher builds stages once per process; the worker rebuilds them per job with that job's settings; and **the dispatcher's fingerprint is the one recorded**. A factory that reads a settings key to choose a provider, and bakes `provider_id` into the stage, silently breaks caching.

Task 3 added a worker-side backstop — `_fingerprint_disagreement` refuses a stage whose fingerprint disagrees with the assignment's — so this now fails loudly rather than poisoning the cache. Do not treat that as licence to be careless: a stage that fingerprints differently between processes will fail **every** job it is given, not some. Keep `make_stage` free of decisions that depend on settings the stage does not declare in `settings_keys`.

- [ ] **Step 1: Write the failing tests**

```python
def test_a_pasted_story_is_read_verbatim(tmp_path):
    path = tmp_path / "story.txt"
    path.write_text("The train never stopped.\n", encoding="utf-8")
    assert PastedStorySource().fetch(str(path)) == "The train never stopped.\n"


def test_a_missing_story_file_is_a_fatal_provider_error(tmp_path):
    with pytest.raises(ProviderError) as exc:
        PastedStorySource().fetch(str(tmp_path / "absent.txt"))
    assert exc.value.kind is ErrorKind.FATAL, "a missing file will not appear on retry"


def test_the_stage_fingerprint_follows_the_story_digest_not_the_path(tmp_path):
    stage = IngestStory(cas=_cas(tmp_path), settings={})
    fp_a = stage.fingerprint(_ctx(settings={"story_digest": "a" * 64, "story_path": "/x"}))
    fp_b = stage.fingerprint(_ctx(settings={"story_digest": "a" * 64, "story_path": "/y"}))
    fp_c = stage.fingerprint(_ctx(settings={"story_digest": "b" * 64, "story_path": "/x"}))
    assert fp_a == fp_b, "the path must not reach the fingerprint"
    assert fp_a != fp_c, "the digest must reach the fingerprint"
```

- [ ] **Step 2: Run and confirm failure**

Run: `pytest tests/unit/providers/test_pasted_story.py tests/unit/app/stages/test_ingest_story.py -v`
Expected: FAIL, `ModuleNotFoundError`.

- [ ] **Step 3: Implement both**

`PastedStorySource.fetch` reads UTF-8 and raises `ProviderError(provider_id="pasted", kind=ErrorKind.FATAL)` on `OSError`. `IngestStory.run` calls `fetch`, then `self._cas.stage_file(text.encode("utf-8"), kind="text")`, returning `StageResult(artifacts=(ArtifactRef(name="story.txt", kind="text", digest=digest),))`.

- [ ] **Step 4: Run, register the entry point, run the gate**

Add `"story_video:ingest_story" = "ytauto.providers.story.pasted:make_stage"` to `pyproject.toml`, reinstall (`pip install -e ".[dev]"`), then `python scripts/check.py`.

- [ ] **Step 5: Commit**

```bash
git commit -am "feat: add the pasted-story source and the ingest_story stage"
```

---

## Task 5: `synthesize_speech` and `EdgeTtsSynthesizer`

Light review. **This is the first network-touching provider** — its tests must not hit the network.

**Files:**
- Create: `src/ytauto/providers/tts/edge.py`, `src/ytauto/app/stages/synthesize_speech.py`
- Modify: `pyproject.toml` (add `edge-tts`)
- Test: `tests/unit/providers/test_edge_tts.py`

**Interfaces:**
- Consumes: `Narration`, `WordBoundary` (Task 3); artifact `story.txt` (Task 4).
- Produces: stage id `synthesize_speech`, `settings_keys = ("voice", "rate")`, emits `narration.mp3` (kind `audio`) and `boundaries.json` (kind `json`).

### Do not copy Task 4's provider-identity pattern here

Task 4's stage reads `provider_id`/`provider_version` off its injected source's `capabilities` rather than from literals. **That is safe there and unsafe here**, and Task 4's own re-review flagged this task by name as the place it breaks.

It is safe in Task 4 because `make_stage` injects `PastedStorySource()` unconditionally — no branch on settings — so the dispatcher (which builds the stage once per process) and the worker (which rebuilds it per job) always inject the same source and compute the same fingerprint.

The moment a factory picks its provider from settings — `edge-tts` vs Piper vs ElevenLabs — the two processes can inject *different* sources from *different* settings snapshots, and their fingerprints diverge. `registry.py`'s docstring states the rule plainly: a stage's fingerprint "must be a pure function of its `JobContext`, never of anything this factory decided."

Task 3's `_fingerprint_disagreement` catches this loudly rather than silently poisoning the cache — but a stage that fingerprints differently in the two processes fails **every** job it is given, not some.

**For this task:** `make_stage` constructs `EdgeTtsSynthesizer` unconditionally. Provider selection from settings is a Phase 2b concern. If you find yourself writing `if settings["tts_engine"] == ...` inside a factory, stop and report it rather than working around it — that is the design question this plan deliberately deferred.

- [ ] **Step 1: Write the failing tests**

```python
def test_word_boundary_events_become_word_boundaries():
    """edge-tts reports offsets in 100-nanosecond ticks; we store seconds."""
    events = [
        {"type": "WordBoundary", "offset": 1_000_000, "duration": 5_000_000, "text": "Hello"},
        {"type": "audio", "data": b"\x00"},
    ]
    narration = _synthesize_from(events)
    assert narration.boundaries == (WordBoundary(text="Hello", start_s=0.1, duration_s=0.5),)


def test_audio_chunks_are_concatenated_in_order():
    events = [
        {"type": "audio", "data": b"aa"},
        {"type": "audio", "data": b"bb"},
    ]
    assert _synthesize_from(events).audio == b"aabb"


def test_a_network_failure_is_retryable():
    with pytest.raises(ProviderError) as exc:
        _synthesize_raising(ConnectionError("no route to host"))
    assert exc.value.kind is ErrorKind.RETRYABLE


def test_an_unknown_voice_is_fatal():
    with pytest.raises(ProviderError) as exc:
        _synthesize_raising(ValueError("No audio was received. Please verify parameters"))
    assert exc.value.kind is ErrorKind.FATAL, "a typo in a voice name will not fix itself"
```

`_synthesize_from` injects a fake async stream, so no test touches the network.

- [ ] **Step 2: Run and confirm failure**

Run: `pytest tests/unit/providers/test_edge_tts.py -v`
Expected: FAIL, `ModuleNotFoundError: ytauto.providers.tts.edge`.

- [ ] **Step 3: Implement**

`EdgeTtsSynthesizer.synthesize` runs `edge_tts.Communicate(text, voice, rate=rate).stream()` under `asyncio.run`, accumulating audio chunks and `WordBoundary` events. Divide `offset`/`duration` by 10,000,000 to convert 100-ns ticks to seconds.

Add `"edge-tts>=6.1"` to `dependencies` and pin it in the lock step of the commit message.

- [ ] **Step 4: Implement the stage**

`SynthesizeSpeech.run` reads `story.txt` via `ctx.input("ingest_story", "story.txt")` → `self._cas.read_bytes(...)`, calls `synthesize`, then stages two blobs: the audio, and `json.dumps([asdict(b) for b in boundaries])`.

- [ ] **Step 5: Run the gate and commit**

```bash
python scripts/check.py
git commit -am "feat: add the edge-tts synthesizer and the synthesize_speech stage"
```

---

## Task 6: `transcribe` and `EdgeBoundaryTranscriber`

Light review.

**Files:**
- Create: `src/ytauto/providers/transcribe/edge_boundary.py`, `src/ytauto/app/stages/transcribe.py`
- Test: `tests/unit/providers/test_edge_boundary.py`

**Interfaces:**
- Consumes: `Narration` (Task 3), `boundaries.json` (Task 5).
- Produces: stage id `transcribe`, `settings_keys = ()`, emits `word_timings.json` (kind `json`) — a JSON array of `[word, start_s, end_s]`.

- [ ] **Step 1: Write the failing tests**

```python
def test_boundaries_become_start_end_triples():
    narration = Narration(audio=b"", boundaries=(
        WordBoundary(text="Hello", start_s=0.1, duration_s=0.5),
        WordBoundary(text="world", start_s=0.7, duration_s=0.3),
    ))
    assert EdgeBoundaryTranscriber().transcribe(narration) == (
        ("Hello", 0.1, 0.6),
        ("world", 0.7, 1.0),
    )


def test_absent_boundaries_are_fatal_and_say_why():
    """This is the 'you switched to Piper, you now need Whisper' seam."""
    with pytest.raises(ProviderError) as exc:
        EdgeBoundaryTranscriber().transcribe(Narration(audio=b"x", boundaries=None))
    assert exc.value.kind is ErrorKind.FATAL
    assert "boundaries" in str(exc.value)
```

- [ ] **Step 2: Run and confirm failure**

Run: `pytest tests/unit/providers/test_edge_boundary.py -v`

- [ ] **Step 3: Implement, run the gate, commit**

```bash
python scripts/check.py
git commit -am "feat: derive word timings from Edge boundary events without ASR"
```

---

## Task 7: `plan_timeline` — the pure core

**Full review.** The most logic-dense code in the system, and it has zero dependencies, so it earns the densest tests and no integration test at all.

**Files:**
- Create: `src/ytauto/core/pipeline/timeline.py`, `src/ytauto/app/stages/plan_timeline.py`
- Test: `tests/unit/core/test_timeline.py`

**Interfaces:**
- Consumes: `word_timings.json` (Task 6).
- Produces: stage id `plan_timeline`, `settings_keys = ("words_per_group_min", "words_per_group_max", "segment_seconds_min", "segment_seconds_max", "seed")`, emits `timeline.json` (kind `json`).

```python
@dataclass(frozen=True)
class CaptionGroup:
    start_s: float
    end_s: float
    words: tuple[tuple[str, float, float], ...]   # (text, start_s, end_s)

@dataclass(frozen=True)
class Segment:
    start_s: float
    end_s: float

@dataclass(frozen=True)
class Timeline:
    duration_s: float
    groups: tuple[CaptionGroup, ...]
    segments: tuple[Segment, ...]

def plan_timeline(
    word_timings: Sequence[tuple[str, float, float]],
    audio_duration_s: float,
    template: Mapping[str, object],
    seed: int,
) -> Timeline: ...
```

- [ ] **Step 1: Write the failing grouping tests**

```python
def test_a_group_closes_at_the_maximum_word_count():
    words = [(f"w{i}", i * 1.0, i * 1.0 + 0.5) for i in range(10)]
    tl = plan_timeline(words, 10.0, _template(words_max=5), seed=1)
    assert [len(g.words) for g in tl.groups] == [5, 5]


def test_a_group_closes_early_on_sentence_ending_punctuation():
    """A caption must not run across a full stop even when it is under the word cap."""
    words = [("The", 0.0, 0.3), ("train.", 0.3, 0.8), ("It", 0.8, 1.0), ("left.", 1.0, 1.4)]
    tl = plan_timeline(words, 1.4, _template(words_max=5), seed=1)
    assert [len(g.words) for g in tl.groups] == [2, 2]


def test_a_group_spans_its_first_word_start_to_its_last_word_end():
    words = [("a", 0.2, 0.4), ("b", 0.5, 0.9)]
    tl = plan_timeline(words, 1.0, _template(words_max=5), seed=1)
    assert (tl.groups[0].start_s, tl.groups[0].end_s) == (0.2, 0.9)


def test_every_word_appears_exactly_once_across_all_groups():
    """Grouping must partition, not sample - a dropped word is a missing caption."""
    words = [(f"w{i}", i * 0.5, i * 0.5 + 0.4) for i in range(23)]
    tl = plan_timeline(words, 12.0, _template(words_max=4), seed=1)
    flat = [w[0] for g in tl.groups for w in g.words]
    assert flat == [w[0] for w in words]
```

- [ ] **Step 2: Run and confirm failure**

Run: `pytest tests/unit/core/test_timeline.py -v`
Expected: FAIL, `ModuleNotFoundError: ytauto.core.pipeline.timeline`.

- [ ] **Step 3: Implement grouping**

Accumulate words; close the group when it reaches `words_per_group_max` **or** the word's text ends with `.`, `!`, `?`, `…` (check after stripping trailing quotes and brackets). `words_per_group_min` is advisory — never split a sentence to reach it.

- [ ] **Step 4: Run and confirm the four pass**

Run: `pytest tests/unit/core/test_timeline.py -v`

- [ ] **Step 5: Write the failing segmentation tests**

```python
def test_a_segment_boundary_always_lands_on_a_group_boundary():
    """A B-roll cut mid-phrase is the artefact this rule exists to prevent."""
    words = [(f"w{i}", i * 0.4, i * 0.4 + 0.35) for i in range(60)]
    tl = plan_timeline(words, 24.0, _template(words_max=4, seg_min=3.0, seg_max=5.0), seed=1)
    group_edges = {g.start_s for g in tl.groups} | {g.end_s for g in tl.groups}
    for seg in tl.segments:
        assert seg.start_s in group_edges or seg.start_s == 0.0
        assert seg.end_s in group_edges or seg.end_s == tl.duration_s


def test_segments_tile_the_whole_duration_without_gap_or_overlap():
    """A gap is a black frame; an overlap is a dropped clip."""
    words = [(f"w{i}", i * 0.4, i * 0.4 + 0.35) for i in range(60)]
    tl = plan_timeline(words, 24.0, _template(seg_min=3.0, seg_max=5.0), seed=1)
    assert tl.segments[0].start_s == 0.0
    assert tl.segments[-1].end_s == pytest.approx(24.0)
    for prev, nxt in zip(tl.segments, tl.segments[1:]):
        assert prev.end_s == nxt.start_s


def test_the_same_seed_produces_an_identical_timeline():
    """An unstable timeline silently disables every downstream cache."""
    words = [(f"w{i}", i * 0.4, i * 0.4 + 0.35) for i in range(40)]
    a = plan_timeline(words, 16.0, _template(), seed=99)
    b = plan_timeline(words, 16.0, _template(), seed=99)
    assert a == b
```

- [ ] **Step 6: Implement segmentation**

Walk the groups, accumulating until the accumulated span reaches at least `segment_seconds_min`; close at the group boundary that first exceeds it, unless doing so would exceed `segment_seconds_max`, in which case close at the previous boundary. The final segment always ends at `duration_s`. Use `random.Random(seed)` for any tie-break so the result is reproducible.

- [ ] **Step 7: Run the whole file**

Run: `pytest tests/unit/core/test_timeline.py -v`
Expected: all seven PASS.

- [ ] **Step 8: Pin the degenerate inputs**

`plan_timeline` is pure with zero dependencies, so its edge cases are the cheapest in the phase to cover — and an off-by-one here surfaces as visibly wrong captions in every video rather than as a test failure.

```python
def test_a_single_word_produces_one_group_and_one_segment():
    tl = plan_timeline([("alone", 0.0, 0.8)], 0.8, _template(), seed=1)
    assert len(tl.groups) == 1
    assert len(tl.segments) == 1
    assert tl.segments[0].end_s == pytest.approx(0.8)


def test_no_words_produces_no_groups_but_still_covers_the_duration():
    """Silence is legal input. A segment list that does not reach duration_s
    leaves the tail of the video black."""
    tl = plan_timeline([], 4.0, _template(), seed=1)
    assert tl.groups == ()
    assert tl.segments[0].start_s == 0.0
    assert tl.segments[-1].end_s == pytest.approx(4.0)


def test_audio_longer_than_the_last_word_still_tiles_to_the_end():
    """edge-tts pads trailing silence; segments must cover it or the last
    B-roll clip ends early and the video goes black before the audio does."""
    tl = plan_timeline([("word", 0.0, 0.5)], 6.0, _template(), seed=1)
    assert tl.segments[-1].end_s == pytest.approx(6.0)


def test_a_zero_length_word_does_not_produce_an_inverted_group():
    """A group whose end precedes its start makes ffmpeg's ass filter drop the
    event silently - a caption that never appears, with nothing failing."""
    tl = plan_timeline([("a", 1.0, 1.0), ("b", 1.0, 1.4)], 2.0, _template(), seed=1)
    for group in tl.groups:
        assert group.end_s >= group.start_s
```

- [ ] **Step 9: Implement the stage and run the gate**

`PlanTimeline.run` reads `word_timings.json`, calls `plan_timeline`, stages `json.dumps(asdict(timeline))`.

Run: `python scripts/check.py`

- [ ] **Step 10: Guard-pin the two rules that matter**

Delete the punctuation check so groups close only on the word cap.
**Predicted:** `test_a_group_closes_early_on_sentence_ending_punctuation` fails with `[4] != [2, 2]`.

Restore it, then change segment closing to a fixed 4.0 s independent of group edges.
**Predicted:** `test_a_segment_boundary_always_lands_on_a_group_boundary` fails on a `seg.start_s` that is in no edge set.

Report both, including the exact assertion text.

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "feat: add plan_timeline, the pure caption and segment planner"
```

---

## Task 8: The `.ass` caption writer

**Full review.** Canvas-parameterised and pure, called by both compose stages — this is what stops approach B duplicating caption logic.

**Files:**
- Create: `src/ytauto/core/captions/__init__.py`, `src/ytauto/core/captions/ass.py`
- Test: `tests/unit/core/test_ass.py`

**Interfaces:**
- Consumes: `Timeline`, `CaptionGroup` (Task 7).
- Produces: `render_ass(timeline: Timeline, *, width: int, height: int, style: Mapping[str, object]) -> str`.

**The highlight technique.** ASS karaoke (`\k`) makes sung words change colour and *stay* changed, which is not the requested look. To highlight only the currently-spoken word, emit **one `Dialogue` event per word**: for a group of N words, event *i* spans word *i*'s `[start, end)` and renders all N words with word *i* wrapped in an accent-colour override. N is 3–5, so this is cheap.

- [ ] **Step 1: Write the failing tests**

```python
def test_the_header_carries_the_canvas_resolution():
    """PlayResX/Y wrong means every caption is mis-scaled on one of the two canvases."""
    ass = render_ass(_timeline(), width=1080, height=1920, style=_style())
    assert "PlayResX: 1080" in ass
    assert "PlayResY: 1920" in ass


def test_one_dialogue_event_per_word_not_per_group():
    tl = _timeline(groups=[_group(words=[("a", 0.0, 0.5), ("b", 0.5, 1.0), ("c", 1.0, 1.5)])])
    ass = render_ass(tl, width=1080, height=1920, style=_style())
    assert ass.count("\nDialogue:") == 3


def test_each_event_accents_exactly_one_word_and_shows_them_all():
    tl = _timeline(groups=[_group(words=[("alpha", 0.0, 0.5), ("beta", 0.5, 1.0)])])
    lines = [ln for ln in render_ass(tl, width=1080, height=1920, style=_style(accent="&H0000FFFF")).splitlines()
             if ln.startswith("Dialogue:")]

    assert len(lines) == 2
    for line in lines:
        assert line.count("&H0000FFFF") == 1, "exactly one word is accented per event"
        assert "alpha" in line and "beta" in line, "the whole group stays on screen"
    # The accent must move: event 0 accents alpha, event 1 accents beta.
    assert lines[0].index("&H0000FFFF") < lines[0].index("beta")
    assert lines[1].index("&H0000FFFF") > lines[1].index("alpha")


def test_event_timings_use_ass_centisecond_format():
    tl = _timeline(groups=[_group(words=[("a", 1.5, 2.25)])])
    line = [ln for ln in render_ass(tl, width=1080, height=1920, style=_style()).splitlines()
            if ln.startswith("Dialogue:")][0]
    assert "0:00:01.50" in line
    assert "0:00:02.25" in line


def test_a_brace_in_the_narration_cannot_inject_an_override_tag():
    """An unescaped { turns caption text into an ASS override block."""
    tl = _timeline(groups=[_group(words=[("{\\\\c&HFF0000&}gotcha", 0.0, 0.5)])])
    ass = render_ass(tl, width=1080, height=1920, style=_style())
    assert "\\\\c&HFF0000&" not in ass.split("Dialogue:")[1]
```

- [ ] **Step 2: Run and confirm failure**

Run: `pytest tests/unit/core/test_ass.py -v`
Expected: FAIL, `ModuleNotFoundError: ytauto.core.captions.ass`.

- [ ] **Step 3: Implement `render_ass`**

Emit `[Script Info]` with `PlayResX`/`PlayResY`, one `[V4+ Styles]` `Style:` line built from `style`, and `[Events]`. Format timestamps as `H:MM:SS.cc`. Escape `{`, `}` and `\` in word text before interpolation — that is what the last test pins.

- [ ] **Step 4: Run and confirm all five pass**

Run: `pytest tests/unit/core/test_ass.py -v`

- [ ] **Step 5: Guard-pin the escaping**

Delete the escaping call.
**Predicted:** `test_a_brace_in_the_narration_cannot_inject_an_override_tag` fails because the raw override survives into the Dialogue line.

- [ ] **Step 6: Run the gate and commit**

```bash
python scripts/check.py
git add -A
git commit -m "feat: add the canvas-parameterised ass caption writer"
```

---

## Task 9: `ytauto broll add` — probe, dual normalise, manifest

**Files:**
- Create: `src/ytauto/infra/broll.py`
- Modify: `src/ytauto/cli/__main__.py`
- Test: `tests/unit/infra/test_broll.py`, `tests/integration/test_broll_ingest.py`

**Interfaces:**
- Consumes: `broll_clips` (Task 2).
- Produces: `normalise_clip(src: Path, *, width: int, height: int, ffmpeg: str) -> list[str]` (arguments, pure); `BrollLibrary(conn, cas).add(path, source_url, licence, attribution, notes) -> str`; `.write_manifest() -> ContentHash`.

Manifest entry shape, which Task 10 and Tasks 11–12 both read:

```json
{"clip_id": "...", "duration_s": 12.5, "source_width": 3840, "source_height": 2160,
 "normalised_landscape_digest": "...", "normalised_vertical_digest": "..."}
```

- [ ] **Step 1: Write the failing argument-construction tests**

```python
def test_normalisation_scales_and_pads_rather_than_stretching():
    """A stretched clip is instantly visible; aspect must be preserved."""
    args = normalise_clip(Path("in.mp4"), width=1080, height=1920, ffmpeg="ffmpeg")
    vf = args[args.index("-vf") + 1]
    assert "force_original_aspect_ratio=decrease" in vf
    assert "pad=1080:1920" in vf


def test_normalisation_drops_the_source_audio():
    """Narration is the only audio track; a stray B-roll track would double up."""
    assert "-an" in normalise_clip(Path("in.mp4"), width=1920, height=1080, ffmpeg="ffmpeg")


def test_normalisation_pins_cfr_and_pixel_format_for_stream_copy():
    args = normalise_clip(Path("in.mp4"), width=1920, height=1080, ffmpeg="ffmpeg")
    assert args[args.index("-r") + 1] == "30"
    assert args[args.index("-pix_fmt") + 1] == "yuv420p"
```

- [ ] **Step 2: Run and confirm failure**

Run: `pytest tests/unit/infra/test_broll.py -v`

- [ ] **Step 3: Implement `normalise_clip` and `BrollLibrary.add`**

`add` probes with the existing `infra/ffmpeg/probe.py`, runs `normalise_clip` **twice** — once per canvas, both from the original source — `put_file`s each result, and inserts one row carrying both digests plus the provenance fields. Wrap the insert in `transaction(conn, immediate=True)`.

- [ ] **Step 4: Write the failing integration test**

```python
@pytest.mark.integration
def test_a_source_clip_is_normalised_to_both_canvases(tmp_path, cas, db_conn):
    src = _lavfi_clip(tmp_path, "testsrc2=size=640x480:rate=25", seconds=2)
    clip_id = BrollLibrary(db_conn, cas).add(src, source_url="local", licence="CC0",
                                             attribution="", notes="")
    row = db_conn.execute("SELECT * FROM broll_clips WHERE id = ?", (clip_id,)).fetchone()
    for digest, expected in (
        (row["normalised_landscape_digest"], (1920, 1080)),
        (row["normalised_vertical_digest"], (1080, 1920)),
    ):
        assert probe_dimensions(cas.path_for(digest)) == expected
```

- [ ] **Step 5: Run it**

Run: `pytest tests/integration/test_broll_ingest.py -v -m integration`
Expected: PASS. A 640×480 source proves both directions of scale-and-pad.

- [ ] **Step 6: Wire the CLI subcommand**

`ytauto broll add <path> --source-url <url> --licence <text> [--attribution <text>] [--notes <text>]`. `--source-url` and `--licence` are **required** — the provenance record is the point, and an optional licence would be blank on every clip within a week. After a successful add, rewrite the manifest.

- [ ] **Step 7: Run the gate and commit**

```bash
python scripts/check.py
git add -A
git commit -m "feat: add B-roll ingest with dual normalisation and provenance"
```

---

## Task 10: `select_broll` and `LibraryVisualStrategy`

Light review.

**Files:**
- Create: `src/ytauto/providers/visual/library.py`, `src/ytauto/app/stages/select_broll.py`
- Test: `tests/unit/providers/test_library_visual.py`

**Interfaces:**
- Consumes: `timeline.json` (Task 7), the manifest (Task 9).
- Produces: stage id `select_broll`, `settings_keys = ("broll_manifest_digest", "seed")`, emits `segments.json` (kind `json`): `[{"clip_id": "...", "in_point_s": 3.0, "duration_s": 4.2}, ...]`, one entry per `Timeline.segment`, in order.

**`segments.json` names `clip_id`, never a digest** — that is what lets one `select_broll` serve both compose stages, each resolving the clip to its own canvas.

- [ ] **Step 1: Write the failing tests**

```python
def test_no_clip_repeats_until_the_library_is_exhausted():
    """Repetition within one video is the most visible quality failure."""
    segments = _select(n_segments=4, clips=_clips(6), seed=1)
    assert len({s["clip_id"] for s in segments}) == 4


def test_selection_wraps_when_there_are_more_segments_than_clips():
    segments = _select(n_segments=5, clips=_clips(2), seed=1)
    assert len(segments) == 5


def test_the_same_seed_selects_the_same_clips():
    assert _select(n_segments=4, clips=_clips(6), seed=7) == _select(n_segments=4, clips=_clips(6), seed=7)


def test_a_clip_shorter_than_its_segment_is_never_chosen_for_it():
    """A short clip would leave the tail of the segment black."""
    segments = _select(n_segments=1, clips=[_clip("short", 1.0), _clip("long", 30.0)],
                       seed=1, segment_seconds=5.0)
    assert segments[0]["clip_id"] == "long"


def test_an_empty_library_is_a_fatal_provider_error():
    with pytest.raises(ProviderError) as exc:
        _select(n_segments=1, clips=[], seed=1)
    assert exc.value.kind is ErrorKind.FATAL
```

- [ ] **Step 2: Run and confirm failure**

Run: `pytest tests/unit/providers/test_library_visual.py -v`

- [ ] **Step 3: Implement**

Shuffle candidates with `random.Random(seed)`, filter to clips at least as long as the segment, draw without replacement, refill the pool when exhausted. `in_point_s` is a seeded random offset within `duration_s - segment_duration`.

- [ ] **Step 4: Run the gate and commit**

```bash
python scripts/check.py
git add -A
git commit -m "feat: select B-roll segments from the library manifest"
```

---

## Task 11: `compose_landscape`

**Full review.** Neither compose stage has ever run; the nvenc and filter-graph surface is entirely unexercised.

**Files:**
- Create: `src/ytauto/infra/ffmpeg/compose.py`, `src/ytauto/app/stages/compose.py`
- Test: `tests/unit/infra/test_compose_args.py`, `tests/integration/test_compose.py`

**Interfaces:**
- Consumes: `segments.json` (Task 10), `timeline.json` (Task 7), `narration.mp3` (Task 5), the manifest (Task 9), `render_ass` (Task 8).
- Produces: `compose_args(*, clips, ass_path, audio_path, out_path, width, height, encoder) -> list[str]`. Stage id `compose_landscape`, `settings_keys = ("broll_manifest_digest", "caption_style", "encoder")`, emits `master_1920x1080.mp4` (kind `video`) and `captions.ass` (kind `text`).

- [ ] **Step 1: Write the failing argument tests**

```python
def test_the_graph_concatenates_then_burns_captions_in_one_pass():
    """Writing an intermediate file is the dominant cause of slow renders."""
    args = compose_args(clips=[_c("a.mp4", 0, 3), _c("b.mp4", 2, 3)], ass_path=Path("c.ass"),
                        audio_path=Path("n.mp3"), out_path=Path("o.mp4"),
                        width=1920, height=1080, encoder="h264_nvenc")
    graph = args[args.index("-filter_complex") + 1]
    assert "concat=n=2:v=1:a=0" in graph
    assert "ass=" in graph
    assert graph.index("concat") < graph.index("ass"), "captions burn after the concat"


def test_each_segment_is_trimmed_at_its_own_in_point():
    args = compose_args(clips=[_c("a.mp4", 4.5, 3.0)], ass_path=Path("c.ass"),
                        audio_path=Path("n.mp3"), out_path=Path("o.mp4"),
                        width=1920, height=1080, encoder="h264_nvenc")
    assert args[args.index("-ss") + 1] == "4.5"
    assert args[args.index("-t") + 1] == "3.0"


def test_the_ass_path_is_relative_so_a_windows_drive_letter_cannot_break_the_filter():
    """ffmpeg filter syntax treats ':' as an argument separator, so 'C:\\x' breaks the graph."""
    args = compose_args(clips=[_c("a.mp4", 0, 3)], ass_path=Path("captions.ass"),
                        audio_path=Path("n.mp3"), out_path=Path("o.mp4"),
                        width=1920, height=1080, encoder="h264_nvenc")
    graph = args[args.index("-filter_complex") + 1]
    assert ":" not in graph.split("ass=")[1].split("[")[0]


def test_the_output_is_cut_to_the_narration_length():
    args = compose_args(clips=[_c("a.mp4", 0, 3)], ass_path=Path("c.ass"),
                        audio_path=Path("n.mp3"), out_path=Path("o.mp4"),
                        width=1920, height=1080, encoder="h264_nvenc")
    assert "-shortest" in args
```

- [ ] **Step 2: Run and confirm failure**

Run: `pytest tests/unit/infra/test_compose_args.py -v`

- [ ] **Step 3: Implement `compose_args`**

One `-ss`/`-t`/`-i` triple per segment, then `-i` for the narration. `-filter_complex` concatenates the video streams, then applies `ass=<relative path>`. Map the filtered video and the narration audio, encode with the probed encoder, `-shortest`.

**Run ffmpeg with `cwd` set to the stage workdir and pass the `.ass` as a bare filename.** This sidesteps the Windows drive-letter colon entirely rather than fighting ffmpeg's escaping rules, which is what the third test pins.

- [ ] **Step 4: Run and confirm all four pass**

Run: `pytest tests/unit/infra/test_compose_args.py -v`

- [ ] **Step 5: Implement the stage**

`ComposeStage(canvas_width, canvas_height, artifact_name)` is one class; `compose_landscape` and `compose_vertical` are two entry points binding different arguments. It reads the manifest, resolves each `clip_id` to the digest **for its own canvas**, calls `render_ass` at its own resolution, writes the `.ass` into the workdir, runs ffmpeg, and stages both the video and the `.ass`.

A non-zero ffmpeg exit raises `ProviderError(kind=ErrorKind.FATAL)` whose message names the stderr log path from Task 1.

- [ ] **Step 6: Write the failing integration test**

```python
@pytest.mark.integration
def test_a_landscape_master_is_rendered_with_burned_captions(tmp_path, cas, db_conn):
    env = _composed_project(tmp_path, cas, db_conn, canvas=(1920, 1080))
    result = env.run_stage("compose_landscape")
    out = cas.path_for(result.artifact("master_1920x1080.mp4").digest)
    assert probe_dimensions(out) == (1920, 1080)
    assert probe_duration(out) == pytest.approx(env.narration_seconds, abs=0.3)
    assert probe_has_audio(out)
```

- [ ] **Step 7: Run it**

Run: `pytest tests/integration/test_compose.py -v -m integration`
Expected: PASS. If nvenc is unavailable on the runner, the encoder chain falls back to `libx264` — assert on dimensions and duration, never on the encoder used.

- [ ] **Step 8: Guard-pin the trim**

Delete the `-ss` emission so every segment starts at 0.
**Predicted:** `test_each_segment_is_trimmed_at_its_own_in_point` fails with `ValueError: '-ss' is not in list`. Note that this is an error rather than an assertion failure — if that is what happens, say so; it still pins the guard, but the test would be stronger asserting on a parsed argument map.

- [ ] **Step 9: Run the gate and commit**

```bash
python scripts/check.py
git add -A
git commit -m "feat: render the landscape master in a single ffmpeg pass"
```

---

## Task 12: `compose_vertical`

**Full review**, but small — Task 11 built the shared class.

**Files:**
- Modify: `src/ytauto/app/stages/compose.py`, `pyproject.toml`
- Test: `tests/integration/test_compose.py`

**Interfaces:**
- Produces: stage id `compose_vertical`, emits `master_1080x1920.mp4` and `captions.ass`.

- [ ] **Step 1: Write the failing test**

```python
@pytest.mark.integration
def test_the_vertical_master_uses_the_vertical_normalised_clips(tmp_path, cas, db_conn):
    """Resolving clip_id to the landscape digest here would letterbox every segment."""
    env = _composed_project(tmp_path, cas, db_conn, canvas=(1080, 1920))
    result = env.run_stage("compose_vertical")
    out = cas.path_for(result.artifact("master_1080x1920.mp4").digest)
    assert probe_dimensions(out) == (1080, 1920)


@pytest.mark.integration
def test_both_canvases_render_from_one_select_broll_result(tmp_path, cas, db_conn):
    env = _composed_project(tmp_path, cas, db_conn)
    land = env.run_stage("compose_landscape")
    vert = env.run_stage("compose_vertical")
    assert land.artifact("captions.ass").digest != vert.artifact("captions.ass").digest, (
        "each canvas needs its own PlayResX/Y"
    )
```

- [ ] **Step 2: Run and confirm failure**

Run: `pytest tests/integration/test_compose.py -v -m integration -k vertical`
Expected: FAIL, no entry point registered as `story_video:compose_vertical`.

- [ ] **Step 3: Register the second entry point and its factory**

```python
def make_compose_vertical(*, cas, settings):
    return ComposeStage(cas=cas, settings=settings, stage_id="compose_vertical",
                        width=1080, height=1920, artifact_name="master_1080x1920.mp4",
                        digest_field="normalised_vertical_digest")
```

- [ ] **Step 4: Run, gate, commit**

```bash
python scripts/check.py
git add -A
git commit -m "feat: render the vertical master from the same upstream artifacts"
```

---

## Task 13: `ytauto project` and `ytauto run`

**Files:**
- Modify: `src/ytauto/cli/__main__.py`
- Create: `src/ytauto/app/services/enqueue.py`
- Test: `tests/unit/cli/test_run_command.py`

**Interfaces:**
- Produces: `ytauto project create --slug <s> --title <t> --story <path>`; `ytauto run --project <slug> [--max-ticks N]`.

`project create` hashes the story file, `put_bytes` it into the CAS, and stores the digest in `projects.story_digest` **and** in `settings_json` as `story_digest`, alongside `story_path`. That double-write is what makes `ingest_story`'s fingerprint pure (Task 4) while keeping the story readable on disk.

### Hash the normalised text, not the raw file bytes

Found by Task 4's review, and it is a real cache defect if ignored.

`PastedStorySource.fetch` reads with `Path.read_text(encoding="utf-8")`, which performs universal-newline translation — `\r\n` and `\r` both become `\n`. That is required by Task 4's pinned verbatim test and is correct. But **every CAS hashing path in this repo hashes raw bytes**: `hash_file` opens `"rb"`, and `stage_file` hashes what it is handed.

So if this command hashes the file directly, a CRLF story and an LF story with identical text get **different** `story_digest` values — and on Windows, this project's own platform, a story saved from a typical editor is routinely CRLF. Two identical stories would fingerprint differently and spuriously miss the cache, even though `ingest_story` stages byte-identical `story.txt` for both.

Compute it over the normalised text:

```python
text = story_path.read_text(encoding="utf-8")
story_digest = hash_bytes(text.encode("utf-8"))
```

CRLF and LF versions of the same story then hash identically — which is the correct semantics, because they *are* the same story — and the digest agrees with what `ingest_story` actually stages.

- [ ] **Step 0: Pin this before anything else**

```python
def test_the_story_digest_ignores_line_ending_convention(tmp_path):
    """A CRLF and an LF copy of one story are the same story and must
    fingerprint identically, or every Windows-authored story misses the cache."""
    lf = tmp_path / "lf.txt"
    crlf = tmp_path / "crlf.txt"
    lf.write_bytes(b"The train never stopped.\nIt kept going.\n")
    crlf.write_bytes(b"The train never stopped.\r\nIt kept going.\r\n")

    assert story_digest_for(lf) == story_digest_for(crlf)
```

**Guard-pin it:** switch `story_digest_for` to `hash_file(path)` (raw bytes). **Predicted:** the test fails with two differing 64-character hex digests — not an error. Report if it fails any other way.

- [ ] **Step 1: Write the failing test**

```python
def test_run_enqueues_one_job_and_drains_it(tmp_path, db_conn):
    _project(db_conn, slug="ghost-train")
    rc = main(["--data-dir", str(tmp_path), "run", "--project", "ghost-train", "--max-ticks", "20"])
    assert rc == 0
    assert _job_state(db_conn, slug="ghost-train") == "succeeded"


def test_run_on_an_unknown_slug_exits_nonzero_without_enqueueing(tmp_path, db_conn):
    rc = main(["--data-dir", str(tmp_path), "run", "--project", "absent"])
    assert rc == 2
    assert db_conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0
```

- [ ] **Step 2: Run and confirm failure**

Run: `pytest tests/unit/cli/test_run_command.py -v`
Expected: FAIL — `parser.error("unknown command: run")` currently returns 2 for every unknown command, so assert on the job count to distinguish.

- [ ] **Step 3: Implement both subcommands**

`run` builds the pipeline via `build_pipeline`, constructs `Dispatcher`, enqueues one job, and calls `run_until_idle(max_ticks=...)`. Return 0 when the job reaches `succeeded`, 1 when `failed`, 2 on bad input.

- [ ] **Step 4: Run the gate and commit**

```bash
python scripts/check.py
git add -A
git commit -m "feat: add the project and run CLI commands"
```

---

## Task 14: The exit criteria

**Files:**
- Create: `tests/integration/test_first_light.py`

These are the four success criteria from spec §1.3, as executable tests. **Synthetic B-roll throughout** — the criteria must not depend on sourcing footage.

- [ ] **Step 1: Write criterion 1 — two playable files**

```python
@pytest.mark.integration
def test_a_pasted_story_becomes_two_playable_videos(first_light_env):
    env = first_light_env(story="The train never stopped. It just kept going.", clips=4)
    assert env.run() == 0
    for name, dims in (("master_1920x1080.mp4", (1920, 1080)),
                       ("master_1080x1920.mp4", (1080, 1920))):
        path = env.artifact_path(name)
        assert probe_dimensions(path) == dims
        assert probe_duration(path) == pytest.approx(env.narration_seconds, abs=0.5)
        assert probe_has_audio(path)
```

- [ ] **Step 2: Write criterion 2 — a re-run spawns nothing**

```python
@pytest.mark.integration
def test_rerunning_the_same_job_spawns_no_workers(first_light_env):
    """Every stage must be a cache hit. A single spawn means a fingerprint is unstable."""
    env = first_light_env(story="The train never stopped.", clips=4)
    env.run()
    report = env.run_again()
    assert report.spawned == (), f"unstable fingerprints in: {report.spawned}"
    assert len(report.skipped) == 7
```

- [ ] **Step 3: Write criterion 3 — kill and resume**

```python
@pytest.mark.integration
def test_killing_a_worker_mid_render_resumes_at_that_stage(first_light_env):
    env = first_light_env(story="The train never stopped.", clips=4)
    env.run_until_stage("compose_landscape")
    env.kill_running_worker()
    env.fast_forward_backoff()
    assert env.run() == 0
    assert env.spawn_count("synthesize_speech") == 1, "a completed stage must not re-run"
    assert env.spawn_count("compose_landscape") == 2
```

- [ ] **Step 4: Write criterion 4 — selective invalidation**

```python
@pytest.mark.integration
def test_changing_the_caption_colour_rerenders_only_the_compose_stages(first_light_env):
    env = first_light_env(story="The train never stopped.", clips=4)
    env.run()
    env.set_setting("caption_style", {"accent": "&H000000FF"})
    report = env.run_again()
    assert set(report.spawned) == {"compose_landscape", "compose_vertical"}


@pytest.mark.integration
def test_changing_the_voice_does_not_rerun_ingest_story(first_light_env):
    """The settings projection is what makes this true - see stage_support."""
    env = first_light_env(story="The train never stopped.", clips=4)
    env.run()
    env.set_setting("voice", "en-GB-RyanNeural")
    report = env.run_again()
    assert "ingest_story" not in report.spawned
    assert "synthesize_speech" in report.spawned
```

- [ ] **Step 5: Run the whole suite**

Run: `python scripts/check.py`
Expected: ALL CHECKS PASSED.

- [ ] **Step 6: Watch one with your own eyes**

Run `ytauto project create` and `ytauto run` on a real story with real clips, and open both files. The automated criteria prove the pipeline; only this proves it is watchable. Record what looks wrong — caption size, cut rhythm, contrast — as 2b input rather than fixing it here.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "test: pin the four Phase 2a exit criteria end to end"
```

---

## Self-Review Notes

**Spec coverage.** Every section of the spec maps to a task: §4.1 → Task 3; §4.2 → Task 3 (pinned by Task 14 step 4); §4.3 → Task 3; §4.4 → Task 3; §4.5 → Task 2; §5.1 → Task 9; §5.2 → Tasks 9, 10; §6 → Task 1; §7 → Task 7; §8 → Tasks 11, 12; §9 → error kinds asserted in Tasks 4, 5, 6, 10, 11; §10 → Task 14; §3 stage table → Tasks 4–12.

**One deliberate omission.** Spec §9 lists "normalised clip blob missing → FATAL" but no task tests it; it is unreachable in 2a because the Evictor is unwired, and a test would have to fabricate the state. Left to 2b with the Evictor.

**Naming consistency checked.** `settings_keys`, `stage_fingerprint`, `project_settings`, `build_stage`, `build_pipeline`, `normalise_clip`, `compose_args`, `render_ass`, `plan_timeline` are used identically wherever they appear. `clip_id` (never `digest`) is the segment reference throughout Tasks 9–12.

