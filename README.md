# ytauto

Turns written stories into finished, narrated, subtitled YouTube videos — in
Shorts and landscape formats — without supervision. Built for batch operation:
20–100 videos a day, one machine, cheap.

Windows desktop app, Python 3.12 + PySide6. The engine core is Qt-free and runs
headless; the GUI is a client of it, not the other way round.

**Status:** Phase 1a complete (domain models, pipeline DAG, content-addressed
store, fingerprint cache). Phase 1b in progress (job queue, resource governor,
worker subprocesses, dispatcher). No video comes out the far end yet — that's
Phase 2.

---

## Getting started

You need **Python 3.12+** and **git**. Everything else `ytauto doctor` will tell
you about.

```bash
git clone https://github.com/blanc86/youtube-automation.git
cd youtube-automation
python -m venv .venv
.venv\Scripts\pip install -e ".[dev]"
```

Then ask the app what's missing:

```bash
.venv\Scripts\ytauto doctor
```

`doctor` is the onboarding path — it checks nine things (Python, data
directories, database schema, cache ceiling, ffmpeg, the H.264 encoder,
subtitle burn-in, GPU, free disk) and prints a row per check. It exits 0 only
when the environment is green, and it is written to diagnose a broken
environment without crashing on one.

### What `doctor` will probably ask you for

**ffmpeg** (with `ffprobe` beside it) — not bundled. Put it on `PATH`, or point
`YTAUTO_FFMPEG_DIR` at the directory containing the binaries:

```bash
set YTAUTO_FFMPEG_DIR=C:\ffmpeg\bin
```

**An NVIDIA GPU** is optional but wanted. `doctor` reports which H.264 encoder
it found; the render pipeline prefers `h264_nvenc`, falls back to `h264_qsv`,
then `libx264`. Without a GPU everything still works, more slowly.

You do **not** need any API keys yet. Providers land in Phase 2.

---

## Running the checks

One command runs everything CI runs:

```bash
powershell -ExecutionPolicy Bypass -File scripts/check.ps1
```

That's ruff (lint), ruff format, mypy `--strict`, import-linter, then pytest —
unit and integration as separate steps. It prints `ALL CHECKS PASSED` or fails.

Unit tests are hermetic and run anywhere; they mock ffmpeg rather than shelling
out to it. Integration tests are marked and excluded by default:

```bash
.venv\Scripts\python -m pytest                      # unit only
.venv\Scripts\python -m pytest -m integration       # the real-binary ones
```

---

## Layout

| Path | What lives there |
|---|---|
| `src/ytauto/core/` | Domain. Imports **nothing** but the standard library — enforced by import-linter |
| `src/ytauto/infra/` | SQLite, content-addressed store, logging, ffmpeg, GPU detection |
| `src/ytauto/app/` | Scheduler: queue, governor, worker protocol, dispatcher |
| `src/ytauto/cli/` | `ytauto` entry point, including `doctor` |
| `docs/superpowers/specs/` | Design documents — the "why" |
| `docs/superpowers/plans/` | Implementation plans, task by task |

Three architectural rules are enforced by the build, not by convention:
`core/` depends on nothing internal, nothing below `ui/` imports Qt, and the
layering `ui → app → core` holds. Breaking one fails CI.

---

## Where to read first

- `docs/superpowers/specs/2026-07-30-youtube-automation-design.md` — the whole
  system: ports, pipeline, governor, process model, render strategy.
- `docs/superpowers/phase-1a-carry-forward.md` — what the last phase learned,
  including the traps the current phase has to avoid. Short and worth it.
- `CONTRIBUTING.md` — how work actually gets done here.

---

## Scope

**In:** story ingestion, script rewriting, TTS, word-level subtitles, B-roll
selection and rendering, dual-format export, batch queueing.

**Out for v1:** uploading to YouTube. The Data API allows roughly six uploads a
day against its quota, which cannot keep pace with rendering, so publishing
stays manual. A `Publisher` port is reserved for when that changes.
