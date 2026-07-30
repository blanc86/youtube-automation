# Faceless YouTube Video Automation — Architecture & Design Spec

**Date:** 2026-07-30
**Status:** Approved for implementation planning
**Codename:** `ytauto`

---

## 1. Purpose & Scope

A production-grade Windows desktop application (Python + PySide6) that turns a story —
fetched from Reddit, imported from a file, or typed by hand — into a finished, subtitled,
narrated YouTube video in both Shorts (9:16) and Landscape (16:9) formats.

The application is built for **unattended batch operation** at 20–100 videos/day while
remaining pleasant for producing a single video interactively.

### 1.1 In scope

- Story ingest (Reddit API, text files, manual entry)
- AI rewriting via interchangeable LLM providers
- Human script editing with revision history
- TTS narration via interchangeable engines
- Automatic word-level subtitles
- Visual assignment (B-roll library first; AI images second)
- FFmpeg video composition with GPU acceleration
- Thumbnail generation
- Dual-format export
- Project persistence, reopen, and re-edit
- Batch export queue with crash-resume

### 1.2 Explicitly out of scope (v1)

- **Uploading to YouTube.** The YouTube Data API charges 1,600 quota units per upload
  against a 10,000/day default allocation — a hard ceiling of ~6 uploads/day regardless of
  render throughput. A `Publisher` port is reserved in `core/ports/publisher.py` so this can
  be added without structural change, but no implementation ships in v1.
- Multi-machine / networked rendering. The architecture preserves the upgrade path
  (see §3.1) but does not implement it.
- Packaged installer, code signing, auto-update. Deferred to Phase 9; the codebase carries
  the *constraints* that make packaging painless from day one.

### 1.3 Success criteria

1. A non-technical operator produces a finished Short from a Reddit URL in under 5 clicks.
2. A 40-video overnight batch that crashes at video 31 resumes at video 31.
3. Marginal cost per video stays under **$0.01** on the default provider set.
4. Editing one line of a script and re-exporting reuses all unaffected artifacts.
5. Adding a new TTS engine requires **zero** changes to `core/` or `app/`.

---

## 2. Operating Constraints

Measured on the target machine, 2026-07-30. These are not background details; each one
drives a specific design decision.

| Resource | Measured | Design consequence |
|---|---|---|
| GPU | NVIDIA RTX 3050 Laptop, **4 GB VRAM** | GPU work serialized through a lease broker (§3.5). SDXL infeasible; SD 1.5 only. |
| FFmpeg | 7.1.1, NVENC + QSV + CUDA + libass | Hardware encode and libass subtitle burn-in both available. Encoder fallback chain viable. |
| CPU / RAM | i7-11370H, 8 logical cores / 16 GB | Worker pool default = 2, hard cap 3. |
| Disk | **84 GB free** | Asset cache requires LRU eviction with a size ceiling from day one, not later. |
| Python | 3.10.0 installed | **Target 3.12.** Measurable interpreter gains and better typing syntax. Treated as a Phase 0 prerequisite. |

### 2.1 Decisions taken

| Decision | Choice | Rationale |
|---|---|---|
| Throughput model | Persistent queue + worker subprocess pool | Batch and crash-resume come free; scales down to one video without penalty |
| Cost posture | Cheap-cloud LLM + free local everything else | ~$0.003/video; the rewrite is the only paid call |
| Primary visuals | B-roll library; AI images Phase 8 | Zero marginal cost, fastest render, genre convention |
| Distribution | Single user now, shippable later | ~15% overhead throughout, avoids a painful retrofit |
| Execution | Monolith app, Qt-free engine, subprocess workers | Full capability of a daemon at materially lower complexity |

---

## 3. Architecture

### 3.1 Layering

```
┌──────────────────────────────────────────────────────────┐
│ ui/          PySide6 views + viewmodels                  │
│              the ONLY layer permitted to import Qt       │
├──────────────────────────────────────────────────────────┤
│ app/         use cases, scheduler, registry, event bus   │
├──────────────────────────────────────────────────────────┤
│ core/        domain models + PORTS (typing.Protocol)     │
│              pure python — no I/O, no Qt, no network     │
├──────────────────────────────────────────────────────────┤
│ providers/   adapters implementing ports                 │
│ infra/       SQLite, CAS, ffmpeg, keyring, http, logging │
└──────────────────────────────────────────────────────────┘
```

**Dependencies point inward only.** `core` imports nothing from the layers below it.

This rule is enforced by an `import-linter` contract executed in CI. An architecture rule
that is not executable decays within a month; this one fails the build.

The direct payoff: the engine runs headless, is testable without a display, drives a CLI
for free, and can later be lifted into a daemon by swapping the worker transport rather
than rewriting the core.

### 3.2 Ports — the plugin seams

Seven active provider families plus one reserved seam, each a `typing.Protocol` in
`core/ports/`:

| Port | Purpose | v1 implementations |
|---|---|---|
| `StorySource` | fetch/import stories | `reddit`, `textfile`, `manual` |
| `ScriptGenerator` | rewrite story into script | `gemini`, `claude`, `openai`, `ollama` |
| `SpeechSynthesizer` | text → narration audio | `edge`, `piper`, `elevenlabs` |
| `Transcriber` | audio → word timings | `edge_boundary`, `faster_whisper` |
| `VisualStrategy` | populate timeline visuals | `broll_loop`, `ai_images` (Ph. 8) |
| `ImageGenerator` | prompt → image | `sd_local`, `fal` (Ph. 8) |
| `ThumbnailRenderer` | compose thumbnail | `pillow` |
| `Publisher` | *reserved — not implemented* | — |

### 3.3 Capability descriptors and cost policy

Every provider ships a declarative descriptor:

```python
@dataclass(frozen=True)
class CapabilityDescriptor:
    provider_id: str
    version: str
    cost_model: CostModel          # per_token | per_char | per_second | free
    latency_class: LatencyClass    # instant | fast | slow
    offline: bool
    requires_gpu: bool
    vram_mb: int | None
    quality_tier: int              # 1..5
    rate_limit: RateLimit | None
    languages: frozenset[str]
```

`core/policy/selection.py` resolves `(task, constraints) -> provider`. `core/policy/cost.py`
holds a `BudgetLedger` enforcing per-project and per-day ceilings.

This is the mechanism that makes "keep operating costs extremely low" a **system property
rather than an intention**: the default policy prefers `offline` and `free` providers and
escalates to paid ones only on explicit opt-in, and the ledger refuses work that would
breach a ceiling.

**Discovery.** First-party providers register in a built-in table. Third-party providers are
discovered via `importlib.metadata.entry_points(group="ytauto.providers")`. Adding a
provider therefore requires zero changes to `core/` or `app/` — this is the stated
requirement, made structural.

### 3.4 Pipeline: content-addressed DAG

```python
class Stage(Protocol):
    id: str
    version: int
    def fingerprint(self, ctx: JobContext) -> str: ...
    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult: ...
```

```
fingerprint = hash(stage_id, stage_version, provider_id, provider_version,
                   input_artifact_hashes, relevant_settings)
```

A stage whose fingerprint matches a stored artifact is **skipped**. Artifacts live in a
content-addressed store keyed by their own hash.

This single mechanism delivers three separate requirements:

- **Crash-resume.** A batch dying at video 31 resumes at 31; completed stages of the
  in-flight video are also retained.
- **Cheap iteration.** Editing one script line invalidates stages 3–9 but not 1–2. Changing
  only caption styling invalidates compose and export alone — audio, timings and visuals
  are reused untouched.
- **Cross-project dedup.** Identical text with an identical voice synthesizes once, ever.

This is the highest-leverage element of the design and should be built before any provider.

### 3.5 Resource governor

A central lease broker in `app/scheduler/governor.py` over four pools:

| Pool | Capacity | Guards |
|---|---|---|
| `gpu_compute` | **1** | Whisper / Stable Diffusion — the 4 GB ceiling |
| `gpu_encode` | 2 | concurrent NVENC sessions |
| `cpu_heavy` | cores − 2 | libx264 fallback, audio processing |
| `net_api` | per-provider token bucket | rate limits, cost pacing |

Workers acquire a lease before heavy work and release on completion or failure (context
manager, released on process death via the dispatcher's reaper).

Without this, concurrent Whisper + NVENC + SD will exhaust 4 GB of VRAM nondeterministically
— the single worst class of bug to diagnose in this system. It also gives the operator a
"pause GPU work" toggle for free.

### 3.6 Render strategy

The performance core. Two rules:

1. **Normalize B-roll once, at ingest.** Every library clip is transcoded a single time to
   the canvas spec (1080×1920, CFR 30 fps, yuv420p, fixed GOP) and stored in the CAS. All
   later use is stream-copy compatible.
2. **One decode, one encode, no intermediates.** Compose is a single FFmpeg invocation:
   concat the normalized segments, apply the `ass` filter for captions, mux narration,
   encode once. Writing intermediate files is the dominant cause of slow render pipelines.

Encoder chain: `h264_nvenc` (`-preset p4 -tune hq -rc vbr -cq 23`) → `h264_qsv` → `libx264`.
Probed once at startup by `infra/ffmpeg/locator.py` and cached in settings.

Dual-format export shares all upstream artifacts — audio, word timings, subtitle file — and
differs only in canvas, crop and a second encode pass.

### 3.7 Process model

```
┌───────────────────────────────┐
│ Main process                  │
│  ├── Qt event loop (ui/)      │
│  ├── Dispatcher thread        │──┐ spawn
│  ├── Event bus                │  │
│  └── SQLite (WAL)             │  │
└───────────────────────────────┘  │
                                   ▼
      ┌────────────────────────────────────────┐
      │ Worker subprocess ×N (default 2)       │
      │  python -m ytauto.app.worker           │
      │  never imports Qt                      │
      │  JSON-lines progress → stdout pipe     │
      └────────────────────────────────────────┘
```

Workers are isolated: a segfault in torch or a hung FFmpeg kills a worker, not the
application. The dispatcher reaps dead workers, releases their leases, and requeues the job
from its last completed stage.

SQLite runs in WAL mode. The main process writes job state; workers report through the pipe
rather than writing concurrently, keeping the write path single-threaded and avoiding lock
contention.

---

## 4. Folder Structure

```
youtube-automation/
├── pyproject.toml                    # deps + tool config, single source
├── README.md
├── docs/superpowers/specs/
├── src/ytauto/
│   ├── core/                         # PURE DOMAIN — no I/O, no Qt, no network
│   │   ├── models/
│   │   │   ├── project.py            Project, ProjectMeta, ProjectRef
│   │   │   ├── story.py              Story, SourceMeta
│   │   │   ├── script.py             Script, Beat, ScriptRevision
│   │   │   ├── timeline.py           Timeline, VisualSegment, CaptionCue
│   │   │   ├── asset.py              Asset, AssetKind, ContentHash
│   │   │   ├── job.py                Job, JobStage, JobState, StageResult
│   │   │   ├── render.py             RenderSpec, Format, EncoderSpec
│   │   │   └── settings.py           typed settings tree
│   │   ├── ports/
│   │   │   ├── story_source.py       script_generator.py
│   │   │   ├── speech_synthesizer.py transcriber.py
│   │   │   ├── visual_strategy.py    image_generator.py
│   │   │   ├── thumbnail_renderer.py publisher.py      # reserved
│   │   │   └── capability.py         CapabilityDescriptor, CostModel
│   │   ├── pipeline/
│   │   │   ├── stage.py graph.py fingerprint.py
│   │   │   └── stages/
│   │   │       ingest.py rewrite.py synthesize_speech.py transcribe.py
│   │   │       plan_timeline.py acquire_visuals.py compose_video.py
│   │   │       render_thumbnail.py export.py
│   │   ├── policy/                   cost.py selection.py
│   │   ├── errors.py                 typed error taxonomy
│   │   └── events.py                 domain events
│   ├── app/                          # ORCHESTRATION — still Qt-free
│   │   ├── services/                 project script voice render asset template
│   │   ├── scheduler/
│   │   │   queue.py dispatcher.py governor.py worker_protocol.py
│   │   ├── worker/__main__.py runner.py
│   │   ├── registry.py               entry-point discovery
│   │   ├── container.py              composition root
│   │   └── bus.py
│   ├── providers/
│   │   ├── sources/                  reddit.py textfile.py manual.py
│   │   ├── llm/                      base.py gemini.py claude.py openai.py ollama.py
│   │   ├── tts/                      base.py edge.py piper.py elevenlabs.py
│   │   ├── asr/                      edge_boundary.py faster_whisper.py
│   │   ├── visual/                   broll_loop.py ai_images.py
│   │   ├── image/                    sd_local.py fal.py
│   │   └── thumbnail/                pillow_renderer.py
│   ├── infra/
│   │   ├── db/                       engine.py migrations/ repositories/
│   │   ├── cas/                      store.py eviction.py
│   │   ├── ffmpeg/                   locator.py probe.py command.py runner.py filters.py
│   │   ├── secrets/                  keyring_store.py
│   │   ├── http/                     client.py
│   │   ├── paths.py logging.py
│   ├── ui/                           # ONLY layer importing PySide6
│   │   ├── main_window.py
│   │   ├── shell/                    navigation.py titlebar.py
│   │   ├── views/                    dashboard projects script_editor voice_studio
│   │   │                             video_builder asset_manager export_queue settings
│   │   ├── viewmodels/               testable without a display
│   │   ├── widgets/ theme/
│   │   └── bridge.py                 engine events → Qt signals
│   └── cli/__main__.py               headless: render, batch, doctor
├── tests/                            unit/ contract/ integration/ golden/
├── assets/                           fonts/ templates/ icons/
└── scripts/                          bootstrap.py doctor.py
```

**`src/` layout** is deliberate: it forces tests to run against the *installed* package rather
than a directory that happens to sit on `sys.path`, surfacing missing-data-file bugs long
before packaging day — which matters because this ships to others later.

**`ui/viewmodels/`** as a distinct layer keeps views dumb and logic testable headlessly, so
GUI behaviour does not become the untested region of the codebase.

---

## 5. Module Breakdown

### 5.1 `core/` — domain

Pure data structures and protocols. No I/O of any kind. Every model is a frozen dataclass
with an explicit `content_hash()`. Fully unit-testable in milliseconds.

**Depends on:** nothing but the standard library.

### 5.2 `core/pipeline/` — stage framework

The `Stage` protocol, the DAG, topological planning, and canonical fingerprint hashing.
Fingerprint canonicalisation (stable key ordering, float normalisation, path exclusion) is
subtle and gets dedicated tests — a fingerprint that varies spuriously silently destroys all
caching benefit.

**Depends on:** `core/models`.

### 5.3 `app/scheduler/` — execution

`queue.py` (persistent, claim-with-lease semantics), `dispatcher.py` (spawns and reaps
workers), `governor.py` (resource leases), `worker_protocol.py` (versioned JSON-lines
message schema for progress, logs, results, errors).

**Depends on:** `core`, `infra/db`.

### 5.4 `app/registry.py` — provider resolution

Built-in table plus entry-point discovery, capability validation at load, and construction
of provider instances with injected config and secrets.

### 5.5 `providers/` — adapters

Each provider is self-contained and implements exactly one port. Every provider must pass
the shared **contract test suite** for its port (§7.2). Providers never import each other.

### 5.6 `infra/`

- `cas/` — content-addressed store, `assets/cas/ab/cdef…`, refcounts in SQLite,
  LRU eviction to a ceiling of `min(40 GB, 40% of free space)`. NTFS hardlinks into project
  directories with copy fallback.
- `ffmpeg/` — locator (PATH → bundled → configured), capability probe, a typed command
  builder, and a runner that parses `-progress` output into structured events. The command
  builder is a pure function returning `list[str]`, making FFmpeg invocations
  snapshot-testable without executing anything.
- `secrets/` — OS keyring; API keys never touch config files or logs.

### 5.7 `ui/` — presentation

Eight views matching the requested surface: Dashboard, Projects, Script Editor, Voice
Studio, Video Builder, Asset Manager, Export Queue, Settings. Views bind to viewmodels;
viewmodels consume engine events through `bridge.py`, which is the *only* place engine
events become Qt signals.

---

## 6. Data Flow

### 6.1 Pipeline stages

| # | Stage | Port | Output artifact | Marginal cost |
|---|---|---|---|---|
| 1 | `ingest` | `StorySource` | `Story` | free |
| 2 | `rewrite` | `ScriptGenerator` | `Script` (hook, beats, CTA) | **~$0.003** |
| — | *human gate* | — | `ScriptRevision` | free |
| 3 | `synthesize_speech` | `SpeechSynthesizer` | `narration.wav` + boundaries | free |
| 4 | `transcribe` | `Transcriber` | `WordTiming[]` | free |
| 5 | `plan_timeline` | *pure function* | `Timeline` | free |
| 6 | `acquire_visuals` | `VisualStrategy` | segment → asset refs | free |
| 7 | `compose_video` | ffmpeg | `master.mp4` | free (GPU) |
| 8 | `render_thumbnail` | `ThumbnailRenderer` | `thumb.jpg` | free |
| 9 | `export` | ffmpeg | Shorts + Landscape | free (GPU) |

The rewrite is the only billed call in the pipeline.

### 6.2 Word timings without ASR

`edge-tts` emits `WordBoundary` events during synthesis, carrying per-word offsets and
durations — free, instant, and requiring no GPU. On the default path this removes automatic
speech recognition from the pipeline entirely.

`Transcriber` therefore has two implementations behind one port:

- `EdgeBoundaryTranscriber` — default; consumes metadata already produced by stage 3.
- `FasterWhisperTranscriber` — universal fallback, required for Piper, ElevenLabs and
  imported audio, and the only one that consumes a `gpu_compute` lease.

### 6.3 `plan_timeline` as a pure function

`(audio_duration, beats, word_timings, template) -> Timeline`

Deliberately I/O-free. This is the most logic-dense and most iteration-prone part of the
system — caption grouping, segment cut points, beat alignment — so it must be testable with
zero setup and no external dependencies.

### 6.4 Persistence

**SQLite** (WAL) holds: `projects`, `stories`, `scripts`, `script_revisions`, `artifacts`,
`cas_objects`, `jobs`, `job_stages`, `provider_state` (circuit breakers), `budget_ledger`,
`templates`, `broll_clips`, `broll_usage`, `settings`.

`broll_usage` tracks which library segments a project consumed, so segment selection avoids
repetition within a video and across recent videos — a quality detail that matters for
channel output and is trivial to add here, painful to retrofit.

**Project directory** — `projects/<slug>/` holds a human-readable `project.json` plus
hardlinks into the CAS. A project is reopenable from disk alone; the database is an index
and a cache, never the sole source of truth for user content.

### 6.5 Invalidation

Editing a script rewrites its content hash, invalidating stages 3–9 and leaving 1–2 intact.
Adjusting caption styling invalidates 7 and 9 only. This is a consequence of §3.4, not
special-cased logic.

---

## 7. Error Handling & Testing

### 7.1 Errors

Typed taxonomy in `core/errors.py`: `ProviderError` with `retryable` / `fatal` /
`rate_limited` / `quota_exceeded` variants, plus `RenderError`, `ValidationError`,
`ResourceExhausted`.

- Exponential backoff with jitter on retryables only; never on `fatal`.
- Per-provider circuit breaker persisted in `provider_state`.
- Declarative fallback chains (`gemini-flash → haiku → ollama`) configured, not coded.
- Every failure captures a **diagnostic bundle**: stage id, fingerprint, provider id and
  version, full FFmpeg command line, stderr tail, environment versions. This is what makes
  a failure at 3 a.m. in a 40-video batch diagnosable the next morning.
- Failed jobs remain queued in a `failed` state, resumable from the failed stage.

### 7.2 Testing

| Layer | Approach |
|---|---|
| `core/` | Pure unit tests, no I/O, millisecond runtime; `mypy --strict` |
| Providers | **Shared contract suite per port** — every implementation of a port runs the same tests; recorded HTTP fixtures so CI never touches the network |
| Fingerprinting | Dedicated stability tests: same inputs → same hash across processes and interpreter restarts |
| FFmpeg | Command builder is pure → snapshot tests on `list[str]` without executing |
| Render | Golden-frame tests: render 3 s, extract frames, perceptual-hash compare with tolerance |
| GUI | `pytest-qt` against viewmodels; minimal widget smoke tests |
| Architecture | `import-linter` contract failing the build on any inward-dependency violation |

The contract suite is the mechanism that keeps the plugin architecture honest: a new TTS
engine is "done" when it passes the same tests every other TTS engine passes.

---

## 8. Development Roadmap

Each phase ends with the module meeting the **Definition of Ready** (§8.1) before the next
begins.

| Phase | Deliverable | Exit criterion |
|---|---|---|
| **0** | Foundation: Python 3.12, `pyproject.toml`, paths, structured logging, SQLite + migrations, CAS, FFmpeg locator/probe, `doctor` CLI, CI with import-linter | `ytauto doctor` reports a green environment |
| **1** | Domain + pipeline framework: models, ports, `Stage`, DAG, fingerprinting, queue, governor, worker protocol. **No providers.** | A synthetic 3-stage job runs, crashes, and resumes correctly |
| **2** | **Vertical slice — first real video, headless.** manual story → Gemini rewrite → Edge TTS → edge boundaries → B-roll → compose → export | `ytauto render story.txt` produces a watchable Short |
| **3** | GUI shell: navigation, theme, Dashboard, Projects, Script Editor | Create, edit, reopen a project entirely through the GUI |
| **4** | Voice Studio, Video Builder, Export Queue with live progress | Full single-video production without touching a terminal |
| **5** | Asset Manager: B-roll ingest + normalization, CAS browser, eviction policy | 20 GB library ingested; cache respects its ceiling under load |
| **6** | Thumbnails, templates, Landscape format | Dual-format export from one timeline |
| **7** | Provider breadth: Claude, OpenAI, Ollama, Piper, ElevenLabs, faster-whisper, Reddit source | All providers pass their port contract suites |
| **8** | AI image visual strategy (SD 1.5 local + hosted) | Horror-genre video with story-matched visuals |
| **9** | Packaging readiness: bundled FFmpeg resolution, PyInstaller spec, crash reporting | Runs on a clean machine with no Python installed |

**Phase 2 is the critical milestone.** It produces a real, watchable video before any GUI
work begins, which validates the pipeline, the fingerprint cache and the render strategy at
the point where changing them is still cheap.

**This document is an umbrella spec.** It defines the contracts between subsystems and the
build order. It is deliberately not a single implementation plan — each phase gets its own
detailed plan written immediately before that phase begins, so later plans can incorporate
what earlier phases actually taught us. Implementation planning starts with Phase 0.

### 8.1 Definition of Ready (per module)

1. Public interface documented; type hints complete; `mypy --strict` clean for `core/`.
2. Unit tests pass; providers additionally pass their port contract suite.
3. All error paths return typed errors — no bare `except`, no silent failures.
4. Structured logging with correlation IDs at every stage boundary.
5. No `TODO` or `FIXME` on the shipped path.
6. `import-linter` contract passes.

---

## 9. Risks

| Risk | Mitigation |
|---|---|
| 4 GB VRAM exhaustion under concurrency | `gpu_compute` semaphore = 1 (§3.5), enforced by lease |
| Disk exhaustion during batch | CAS ceiling + LRU eviction from Phase 0, not retrofitted |
| Fingerprint instability silently disabling all caching | Dedicated cross-process hash stability tests (§7.2) |
| Provider API drift breaking pipelines | Contract suites + recorded fixtures + declarative fallback chains |
| Reddit API terms/rate limits | `StorySource` port isolates it; file and manual sources always work |
| Edge TTS being an undocumented endpoint that may change | `SpeechSynthesizer` port isolates it; Piper is a fully offline fallback |
| GUI work delaying pipeline validation | Phase 2 ships a working video headlessly before any GUI work |
