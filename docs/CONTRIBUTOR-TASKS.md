# Contributor tasks

Ready-to-take work, each self-contained. Copy a section into a new GitHub issue
verbatim — the body is written for someone who has not read this file.

**Before you start:** read `CONTRIBUTING.md`, especially the testing section.
The rule that matters most here is that a test pinning a guard must be
demonstrated failing with that guard deleted. Twelve fix rounds on this project
have all traced to plans rather than implementers, and the dominant failure is a
test that passes for the wrong reason.

## Coordination — read this first

Work is in flight on `phase-1b-queue-and-dispatcher`. **Do not edit these files**
until that branch merges, or you will collide:

```
src/ytauto/infra/db/engine.py          src/ytauto/core/pipeline/*
src/ytauto/infra/db/migrations.py      src/ytauto/core/models/artifact.py
src/ytauto/infra/artifacts.py          src/ytauto/app/scheduler/*  (new)
src/ytauto/infra/cas/store.py          src/ytauto/app/worker.py    (new)
```

Every task below stays clear of those. Claim one by commenting on its issue so
two people don't take the same thing.

---

## 1. Cover the `exc_info` branch in the JSON log formatter

**Difficulty:** small · **Files:** `src/ytauto/infra/logging.py`, `tests/unit/infra/test_logging.py`

`JsonFormatter` has a branch that turns a log record's `exc_info` into an `exc`
field in the JSON payload. Nothing exercises it — every existing test passes
`exc_info=None`. Exception logging is the single most valuable thing that
formatter does, and it is the one path with no coverage.

This matters more than it looks: worker subprocesses will relay their tracebacks
through this formatter, so a bug here loses the diagnostic information from
exactly the failures you most need to debug.

**Done when:** a test logs a real caught exception, asserts the formatted JSON
contains the exception type and message, and fails with the `exc_info` branch
deleted. State the expected failure message in your PR.

---

## 2. Cover the ffmpeg locator's directory-fall-through

**Difficulty:** small · **Files:** `src/ytauto/infra/ffmpeg/locator.py`, `tests/unit/infra/test_ffmpeg_locator.py`

The locator resolves ffmpeg through several candidate sources in precedence
order. The case "the directory exists but contains no ffmpeg, so try the next
candidate" is uncovered, so precedence is only actually tested at the first two
levels.

**Done when:** a test creates an existing-but-empty candidate directory, asserts
resolution falls through to the next source, and fails if the fall-through is
removed.

---

## 3. Make the encoder/filter parser guard structural

**Difficulty:** small · **Files:** `src/ytauto/infra/ffmpeg/probe.py`, `tests/unit/infra/test_ffmpeg_probe.py`

`parse_encoders` and `parse_filters` currently identify real entries by
excluding a separator character — a denylist. A structural constraint such as
`([A-Za-z0-9]\S*)` would be strictly more robust at no cost, because it says
what a valid name *is* rather than what it isn't.

**Done when:** the parsers use a structural pattern, existing tests still pass,
and a new test covers a line the old denylist would have misclassified.

---

## 4. Remove the unreachable `parser.error` branch in the CLI

**Difficulty:** small · **Files:** `src/ytauto/cli/__main__.py`

There is a `parser.error` call that cannot be reached: the argument it guards is
declared `required=True`, so argparse exits before control ever gets there. Dead
code that looks like a safety net is worse than no safety net.

**Done when:** either the branch is removed, or — if you find it *is* reachable —
a test proves it and the branch stays. Say which you found.

---

## 5. Give the `settings` table behavioural coverage

**Difficulty:** small · **Files:** `tests/unit/infra/test_migrations.py`

The `settings` table is created by migration 001 and has never had a single
behavioural test — only an assertion that the table exists. Nothing reads or
writes it yet, which is exactly why now is the cheap time to pin its shape.

**Done when:** tests cover writing and reading a setting, the primary key
rejecting a duplicate key, and whatever `NOT NULL` constraints the schema
declares.

---

## 6. Wire up coverage measurement, or drop the dependency

**Difficulty:** small · **Files:** `pyproject.toml`

`pytest-cov` is installed as a dev dependency with no `--cov` configuration and
no threshold, so it measures nothing. Either configure it with
`--cov-fail-under` (coverage was around 96% at the end of Phase 0, and a
threshold would have flagged the `doctor` branch gaps that shipped) or remove
the dependency.

**Done when:** either coverage is enforced with a threshold that passes today,
or `pytest-cov` is gone from `pyproject.toml`. Both are acceptable; pick one and
say why.

---

## 7. Type-check the test suite

**Difficulty:** medium · **Files:** `pyproject.toml`, various under `tests/`

mypy is configured with `packages = ["ytauto"]`, so `tests/` is never checked.
There are around two dozen `# type: ignore` comments in the test suite that are
therefore never validated — some may be stale, and a stale ignore hides a real
type error.

Expect to fix genuine type errors this uncovers. Do not silence them with more
ignores.

**Done when:** mypy checks `tests/` as well as `src/`, the gate passes, and your
PR lists any real errors the change uncovered.

---

## 8. Add a Linux leg to CI

**Difficulty:** small · **Files:** `.github/workflows/ci.yml`

CI runs Windows and macOS. Linux is untested, and an untested platform is an
unsupported one. Adding `ubuntu-latest` to the matrix should be a one-line
change — but it may surface real path or ffmpeg-availability assumptions, which
is the point.

Note the workflow has an aggregator job named `check` that branch protection
depends on. Do not rename it, and do not remove its `if: always()`, or every PR
will block forever on a status check that never reports.

**Done when:** the matrix includes Linux and all three legs are green.

---

## 9. Cover `CasStore.size_of()` and `touch()`

**Difficulty:** small · **BLOCKED until `phase-1b-queue-and-dispatcher` merges**

Both are untested. `size_of` feeds `total_size`, which feeds the disk-eviction
ceiling — and that ceiling has already failed once in this project's history, so
its inputs deserve coverage.

This touches `src/ytauto/infra/cas/store.py`, which is being restructured on the
Phase 1b branch. **Wait for that merge**, then pick this up. Comment on the issue
if you want it reserved.

---

## 10. Start a provider behind one of the ports

**Difficulty:** medium–large · **Files:** new, under `src/ytauto/providers/`

This is the most valuable independent work available and it cannot collide with
anything, because the files do not exist yet.

`src/ytauto/core/ports/providers.py` defines the plugin seams — protocols for
fetching stories, rewriting scripts, synthesising speech, transcribing,
planning timelines, generating images, rendering and publishing. Each is a
`Protocol` with a capability descriptor. Nothing implements them yet.

Pick one port and write a real adapter behind it. A `StoryFetcher` reading from
Reddit's public JSON endpoints is the easiest genuinely useful one and needs no
API key.

Read `docs/superpowers/specs/2026-07-30-youtube-automation-design.md` §3.2 and
§3.3 first — the capability descriptor drives cost and scheduling decisions, so
declaring it accurately matters as much as the adapter itself.

**Done when:** the adapter satisfies its protocol (`isinstance` against the
runtime-checkable protocol passes), declares an accurate capability descriptor,
has tests that do not hit the network, and the gate passes. Discuss the
interface in the issue before writing much code.

---

## 11. Auto-clipping: turn a long video or stream into short clips

**Difficulty:** large · **Files:** new — a port in `src/ytauto/core/ports/`, a provider under `src/ytauto/providers/`, stages under `src/ytauto/core/pipeline/`

**This is a self-contained feature with a clear owner. Nothing else in the
project touches these files.** Read `docs/EXTENDING.md` before starting.

### What it is

Take a long source — a VOD, a stream recording, a podcast, an uploaded file —
find the segments worth watching, and cut them into short vertical clips with
subtitles burned in. The same output the main pipeline produces, from a
completely different input.

### Why it fits here rather than being a separate project

Most of the machinery already exists and you inherit it for free:

| You need | Already built |
|---|---|
| Word-level timings from speech | The `Transcriber` port — same one the main pipeline uses |
| Deduplicated storage for big media | The content-addressed store, with LRU eviction |
| "Don't redo work I already did" | Fingerprint caching, keyed on inputs + settings + stage version |
| Survive a crash mid-render | Job queue with resume from the last completed stage |
| Cut and encode without quality loss | The render strategy (§3.6 of the design spec) |

What is genuinely missing is **one port and two stages.**

### The seam to design

The new port is a highlight detector. Roughly:

```python
@runtime_checkable
class HighlightDetector(Protocol):
    """Finds segments worth clipping in a transcribed media file."""

    @property
    def capabilities(self) -> CapabilityDescriptor: ...

    def detect(
        self, transcript: tuple[tuple[str, float, float], ...], *, target_s: float
    ) -> tuple[tuple[float, float, float], ...]:
        """Return (start_s, end_s, score) candidates, best first.

        Raises:
            ...
        """
```

Take that as a starting point, not a specification — **propose the signature in
the issue and discuss it before writing much code.** Two questions worth settling
first: does the detector see only the transcript, or also the audio waveform (for
laughter, volume spikes, silence)? And does it return ranges, or ranges plus a
suggested title?

The pipeline then becomes: ingest → extract audio → `Transcriber` → 
`HighlightDetector` → cut segments → subtitle → render. Only the middle two are
new; the rest is the existing path.

### A first implementation that needs no ML

Do not start with a model. A transcript-driven heuristic detector gets
surprisingly far and is testable offline:

- Prefer segments that start on a sentence boundary and end on one.
- Score on speech density, question marks, and gaps that suggest a punchline.
- Reject segments that cut a word in half — you have word-level timings, so this
  is exact rather than approximate.

That is a real, useful deliverable on its own. A smarter detector is then a
second provider behind the same port, swappable without touching the pipeline.

### Scope boundary — read this before you plan

The **render half is not built yet.** Phase 2 delivers the cut-and-encode
pipeline. You can build and fully test the port, the detector, and the stages
that produce clip *ranges* right now — that half is independent and valuable.
Producing actual MP4 files depends on Phase 2 landing.

Say in the issue which half you are taking. Taking only the detection half is a
completely reasonable first PR and will not leave you blocked.

### Done when

- The port is in `core/ports/`, `@runtime_checkable`, stdlib-only, with a
  `Raises:` section on every method.
- At least one detector implements it, declares an accurate
  `CapabilityDescriptor`, and passes `isinstance` against the protocol.
- Tests run offline against a fixture transcript — no network, no model download,
  no real video file in the repo.
- A test proves a segment never splits a word.
- `python scripts/check.py` passes.

---

## 12. Build the provider registry so features are pluggable

**Difficulty:** medium · **Files:** new `src/ytauto/app/registry.py`, plus `pyproject.toml`

`app/registry.py` is specified in the design (§5.4) and does not exist. It is the
one missing piece between "you can add a provider" and "someone else can add a
provider without editing this repo."

It should resolve providers from a built-in table **plus** Python entry-point
discovery, validate each one's capability descriptor at load time, and construct
instances with injected config. Entry points are what let a separate package
ship a provider that this application picks up automatically.

Validating at load matters: a provider declaring `requires_gpu` with no
`vram_mb` cannot be scheduled safely, and the failure should happen at startup
with a clear message rather than mid-render.

**Done when:** a provider declared via an entry point in a separate installable
package is discovered and instantiated, an invalid capability descriptor is
rejected at load with a useful error, and both are tested.

---

## 13. Write the packaging story

**Difficulty:** medium · **Files:** new, plus `pyproject.toml`

There is no way to ship this to someone who does not have a Python toolchain.
Decide and implement an approach — PyInstaller, Briefcase, or something else —
that produces a runnable Windows artifact.

**Done when:** a documented command produces an artifact that runs
`ytauto doctor` successfully on a machine without a development environment.
Write down the tradeoffs you rejected; that reasoning is worth more than the
config.
