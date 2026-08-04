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

## 11. Write the packaging story

**Difficulty:** medium · **Files:** new, plus `pyproject.toml`

There is no way to ship this to someone who does not have a Python toolchain.
Decide and implement an approach — PyInstaller, Briefcase, or something else —
that produces a runnable Windows artifact.

**Done when:** a documented command produces an artifact that runs
`ytauto doctor` successfully on a machine without a development environment.
Write down the tradeoffs you rejected; that reasoning is worth more than the
config.
