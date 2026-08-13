# Phase 2a — First Light: the first watchable video

**Date:** 2026-08-13
**Branch:** `phase-2a-first-light`, from `f0d6524` (Phase 1b merged)
**Supersedes:** parts of `2026-07-30-youtube-automation-design.md` §6.1, as noted in §5.3.
**Governed by:** `docs/superpowers/phase-2-requirements.md`, which is authoritative.

---

## 1. What this is

Phase 2 as scoped spans fourteen independent subsystems. Specifying them in one
document is how a fifth phase arrives without a video. Phase 2a is therefore cut
against one criterion — **the shortest honest path to an MP4 that plays** — and
everything that does not serve it is deferred.

### 1.1 In scope

A pasted story becomes two rendered videos, landscape and vertical, with
narration and word-synchronised captions over B-roll from a local library,
driven from the CLI.

### 1.2 Explicitly deferred to 2b and later

The local web UI, the metadata stage, thumbnails, the n8n handoff manifest, LLM
story generation, the Reddit fetcher, Evictor wiring (and with it finding F3),
`broll_usage` cross-video repetition tracking, and AI-generated video.

The UI is deferred deliberately: a CLI proves the pipeline works, whereas a UI
proves the UI works. Nothing has produced a video in four phases; that is what
2a buys.

### 1.3 Success criteria

1. `ytauto run --project <slug>` on a pasted story and a small B-roll library
   produces two playable files whose duration matches the narration.
2. Re-running the same job spawns **zero** workers — every stage is a cache hit.
3. Killing a worker mid-render and resuming re-runs only the killed stage.
4. Editing the caption colour re-renders only the two compose stages. Editing
   the story re-runs everything. **Changing the voice does not re-run
   `ingest_story`.**

Criterion 4 is the one that fails silently, so it is pinned by tests rather than
inspection.

---

## 2. Decisions taken

| Decision | Choice | Why |
|---|---|---|
| Canvas | **Both** landscape 1920×1080 and vertical 1080×1920 | Anas's call. Upstream artifacts are canvas-agnostic, so only the `.ass` writer and encode step take canvas as a parameter. |
| Captions | Word-by-word highlight, 3–5 word window | The signature look of the genre, and the only option that uses the free per-word timings. |
| Dual-format render | **Two sibling stages** (approach B) | Independent fingerprints, independent failure, and two ffmpeg commands simple enough to paste into a shell. First-render debugging is where 2a's time actually goes. |
| Normalisation | **Twice per clip, once per canvas, both from the original** | See §5.1. A crop from 1920×1080 to 9:16 is 607×1080 and needs a 1.78× upscale — it invents more pixels than it keeps. |
| Worker stderr | **Redirected to a per-attempt log file**, plus a pump-wide deadline | See §6. Deviates from the earlier ruling, with reasons. |
| Story input | Pasted only | Requirements §3 makes pasting first-class; the LLM and fetcher paths are 2b. A pasted story bypasses both, so there is no `rewrite` stage in 2a. |

### 2.1 The GPU is not involved

Nothing on this path runs Whisper. The compose stages take a `gpu_encode` lease;
**no stage takes `gpu_compute`.** Do not design around contention that does not
exist here — it appears only with Piper or ElevenLabs, which are 2b.

---

## 3. The pipeline

One pipeline, `story_video`, seven stages.

| # | Stage | Port → provider | Emits | Depends on |
|---|---|---|---|---|
| 1 | `ingest_story` | `StorySource` → `PastedStorySource` | `story.txt` | — |
| 2 | `synthesize_speech` | `SpeechSynthesizer` → `EdgeTtsSynthesizer` | `narration.mp3`, `boundaries.json` | 1 |
| 3 | `transcribe` | `Transcriber` → `EdgeBoundaryTranscriber` | `word_timings.json` | 2 |
| 4 | `plan_timeline` | pure function, no port | `timeline.json` | 3 |
| 5 | `select_broll` | `VisualStrategy` → `LibraryVisualStrategy` | `segments.json` | 4 |
| 6 | `compose_landscape` | ffmpeg | `master_1920x1080.mp4`, `captions.ass` | 2, 4, 5 |
| 7 | `compose_vertical` | ffmpeg | `master_1080x1920.mp4`, `captions.ass` | 2, 4, 5 |

Stages 6 and 7 form the pipeline's first antichain — `ready_stages` returns both.
They will nevertheless run **sequentially**, because `tick()` claims one job,
picks `ready[0]` and blocks until a terminal message arrives. That is deliberate
and documented in the dispatcher. The value of them being independent nodes is
caching and failure isolation, not concurrency; concurrency becomes a later
scheduler change rather than a pipeline redesign.

Clip normalisation is **not** a stage — it is per-clip, not per-video, and
belongs to `ytauto broll add`. That is what §3.6's "normalize once, at ingest"
means.

`.ass` generation is **not** a stage either — it is a pure function in `core`
parameterised by canvas, called by both compose stages. This is what stops
approach B duplicating the caption logic. Each compose stage additionally
*emits* its `.ass` as a second artifact, so "the captions look wrong" is
debuggable by opening a file rather than re-rendering.

---

## 4. Changes to existing code

### 4.1 Port widening — the free path does not fit through the ports as written

Both defects share a root cause: the ports were designed before anyone
implemented the free path. There are **zero implementations today**, so this
costs nothing now and costs a migration of every provider later.

`SpeechSynthesizer.synthesize(text, *, voice) -> bytes` can only return audio.
But §6.2's entire argument — the one that removes ASR and the GPU from the
default path — requires stage 2 to return narration *and* word boundaries.
There is nowhere in that signature to put them.

`Transcriber.transcribe(audio: bytes)` takes the wrong input for the free
implementation: `EdgeBoundaryTranscriber` wants the boundary events, not audio,
and would have to accept `audio` and ignore it — a signature that invites
someone to pass real audio and wonder why the timings are fabricated.

**Fix.** A new frozen dataclass in `core`:

```python
@dataclass(frozen=True)
class WordBoundary:
    text: str
    start_s: float
    duration_s: float

@dataclass(frozen=True)
class Narration:
    audio: bytes
    boundaries: tuple[WordBoundary, ...] | None
```

`synthesize(...) -> Narration`; `transcribe(narration: Narration) -> tuple[...]`.
`FasterWhisperTranscriber` (2b) ignores `boundaries` and reads `audio`;
`EdgeBoundaryTranscriber` does the reverse and raises
`ProviderError(kind=FATAL)` when `boundaries is None` — which is exactly the
"you switched to Piper, you now need Whisper" case, failing loudly at the seam
instead of silently producing fabricated timings.

### 4.2 Settings must reach a stage, and must be projected before hashing

`dispatcher.tick()` hardcodes `settings={}` into every `JobContext`. Real
settings come from `projects.settings_json`, read when the job is claimed.

Settings feed the fingerprint, so passing the whole blob to every stage means
**changing the caption colour re-runs edge-tts**. §3.4 says
`hash(…, relevant_settings)` and never defines "relevant".

**Fix.** Each stage declares `settings_keys: tuple[str, ...]`. The shared
fingerprint helper (§4.4) projects settings down to those keys before hashing.
Without the projection the cache is technically correct and practically
useless — silent and expensive, so this gets real per-task review.

### 4.3 The stage registry

`runner.py`'s module docstring states a stage "is given its own `CasStore`
reference at construction time", but `worker._load_stage` zero-arg-constructs it,
and `_build_assignment` admits its reflection "only works for a module-level
class with a no-argument constructor — not guaranteed once real provider
parameters exist." A real stage needs a `CasStore` and a provider.

**Fix.** `app/registry.py` (already named in design §5.4) maps
`(pipeline_id, stage_id) -> factory(cas, settings) -> Stage`, and
`(port, provider_name) -> provider factory`. It replaces `stage_import`
reflection; the assignment gains `pipeline_id`. The registry is imported by
**both** processes — the dispatcher constructs stages too, because it calls
`stage.fingerprint(ctx)` for the cache probe.

Provider identity (`provider_id`, `provider_version`) is resolved from the
registry **parent-side**, since the fingerprint is computed there.

### 4.4 The fingerprint helper

`build_spec` has **zero production callers**. The dispatcher fingerprints via
`Stage.fingerprint(ctx)`, leaving each stage on the honour system to hash its own
provider identity and settings. Seven stages hand-rolling that is seven chances
to silently disable caching — the hazard `fingerprint.py`'s own docstring names
and Phase 1a §1.2 burned a phase on.

One helper in `app/`, called by all seven stages, which projects settings per
§4.2 and calls `build_spec` + `compute_fingerprint`. No stage hand-rolls a
fingerprint.

### 4.5 Migration 004

```
projects     id, slug, title, story_digest, settings_json, created_at, updated_at
broll_clips  id, source_digest, normalised_landscape_digest,
             normalised_vertical_digest, duration_s, width, height,
             source_url, licence, attribution, notes, added_at
```

`jobs.project_id` currently references nothing; this gives it a table.

Deliberately **not** added: `stories`, `scripts`, `script_revisions`,
`templates`, `broll_usage`, `budget_ledger`, `provider_state`. None has a
consumer in 2a, and per requirements §1 the schema is cheap to extend for one
user with disposable data.

`licence` is free text alongside `source_url`, not an enum — a fixed vocabulary
would be guessing at what gets downloaded. The columns land now because it is
one column now versus auditing thousands of clips later (requirements §4).

---

## 5. B-roll

### 5.1 Ingest normalises twice

`ytauto broll add <path> --source-url <url> --licence <text>
[--attribution <text>] [--notes <text>]`

Probes the source with the existing `infra/ffmpeg/probe.py`, then transcodes it
**twice from the original** — 1920×1080 and 1080×1920, CFR 30 fps, yuv420p,
fixed GOP, audio discarded — storing both in the CAS and inserting one
`broll_clips` row.

Two transcodes rather than one crop, because a 9:16 crop of 1920×1080 is
607×1080 and must be upscaled 1.78× to reach 1080×1920. Normalising from the
original keeps each canvas optimal and avoids double-generation loss. §3.6's
rule survives in the way that matters: every clip is transcoded once *per
canvas*, and everything downstream is stream-copy compatible.

Sources that do not match a canvas aspect are scaled and padded, preserving
aspect ratio. The source's original dimensions are recorded.

B-roll audio is discarded at ingest — narration is the only audio track, so
there is no mixing stage and no ducking to get wrong.

### 5.2 The library reaches the worker as an artifact, not a query

Workers must never touch SQLite. `ytauto broll add` therefore rewrites a
**manifest blob** into the CAS and stores its digest in the project settings.

One manifest entry per clip:

```
clip_id, duration_s, source_width, source_height,
normalised_landscape_digest, normalised_vertical_digest
```

`select_broll` reads that blob through its injected `CasStore` and fingerprints
over the manifest digest, so adding a clip invalidates selection and nothing
else.

**`segments.json` references `clip_id`, never a digest.** A segment is
canvas-agnostic — it names a clip, an in-point and a duration — and each compose
stage resolves `clip_id` to the digest for *its own* canvas. This is what lets
one `select_broll` serve both compose stages. It follows that the manifest
digest is in the `settings_keys` of both compose stages as well as
`select_broll`.

---

## 6. The stderr deadlock — a deliberate deviation

The carried ruling was "drain both pipes concurrently AND add a pump-wide
deadline." The deadline is implemented exactly as ruled. The concurrent drain is
replaced by **redirecting worker stderr to a per-attempt log file** in the
stage's workdir.

Reasons:

- It removes the deadlock **by construction**. There is no pipe to fill, so no
  buffer to block on, at any volume. Concurrent draining still deadlocks if the
  drain thread dies, and the portable Windows implementation is a second reader
  thread — more moving parts inside the one code path that must never hang.
- It makes ffmpeg's output a file that can be read after a bad render. Today
  stderr goes nowhere and nothing reads it. In 2a this will be wanted constantly.

The **pump-wide wall-clock deadline stays regardless**: a worker that writes
nothing and never exits hangs the dispatcher just as dead. On deadline the
dispatcher kills the process, releases its leases, and charges the attempt
through the existing `_retry_stage`.

This lands as **task 1**, before any stage that shells out to ffmpeg exists.
The measured trigger — 60 KB of stderr — is reached by ffmpeg in seconds at
default log level, so the first render stage would otherwise hit it immediately.

The file handle must be closed on every path. `pyproject.toml` promotes both
`ResourceWarning` and `PytestUnraisableExceptionWarning` to errors specifically
because a leaked pipe shipped once already; a leaked log file is the same class.

---

## 7. `plan_timeline` — the pure core

`(word_timings, audio_duration_s, template, seed) -> Timeline`, no I/O, per §6.3.

`template` is the settings subset this stage declares in `settings_keys`:
`words_per_group_min/max`, `segment_seconds_min/max`. It is passed as a plain
mapping, not a class, so a new knob is a settings key rather than a signature
change.

**Caption groups** accumulate words until 3–5 words *or* sentence-ending
punctuation, whichever comes first, carrying each word's own span so the
highlight can advance within the group.

**Segment cut points** land every 3–5 seconds, snapped to a caption-group
boundary, so a B-roll cut never lands mid-phrase. Both bounds come from settings.

`seed` makes selection reproducible, which is what lets the fingerprint mean
anything at all.

This is the most logic-dense and most iteration-prone code in the system and it
has zero dependencies, so it gets the densest unit tests and no integration test.

---

## 8. Render

Each compose stage is **one** ffmpeg invocation: segments trimmed from the
normalised clips for its canvas → `concat` → `ass` at its canvas → narration
muxed → encoded once. No intermediate files (§3.6 rule 2).

Encoder chain `h264_nvenc` → `h264_qsv` → `libx264`, probed by the existing
`infra/ffmpeg/locator.py`.

Both stages take a `gpu_encode` lease (capacity 2). Neither takes `gpu_compute`.

---

## 9. Error handling

Carry-forward §1.8 records that the dispatcher cannot map failures to
`FATAL`/`RETRYABLE` from `ValidationError` alone, and that nothing in production
raises `ProviderError` or uses `ErrorKind`. 2a is where that changes: **every
provider raises `ProviderError` with an explicit `kind`.**

| Failure | Kind | Why |
|---|---|---|
| edge-tts network failure / timeout | `RETRYABLE` | Transient. |
| edge-tts rejects the voice name | `FATAL` | A retry cannot fix a typo. |
| `EdgeBoundaryTranscriber` given `boundaries is None` | `FATAL` | Configuration error — a non-boundary TTS was selected. |
| ffmpeg non-zero exit | `FATAL` | A bad filter graph will not fix itself; the log file has the reason. |
| Pump deadline kill | `RETRYABLE` | Possibly transient contention. |
| Normalised clip blob missing | `FATAL` | Unreachable in 2a (no Evictor), so it should be loud if it happens. |

**Carried, not fixed here:** if `_release_job_pins` raises mid-loop inside
`_fail_job`'s transaction, the transaction rolls back and `queue.fail` never
applies, leaving the job `running` rather than `failed`. Found by the Phase 1b
re-review. It is the same all-or-nothing shape `_maybe_complete_job` already had;
it becomes reachable when the Evictor is wired, which is 2b, and it is recorded
there as a blocker alongside F3.

---

## 10. Testing

Rigour is dialled per requirements §9 — real per-task review only where a bug is
expensive and silent.

| Area | Treatment |
|---|---|
| `plan_timeline`, `.ass` writer | Dense zero-setup unit tests, both canvases. **Real review.** |
| Settings projection + fingerprint helper (§4.2, §4.4) | **Real review.** Silent and expensive: a wrong projection disables caching while failing nothing. |
| Stderr fix + pump deadline (§6) | **Real review.** Measured deadlock; a regression hangs the dispatcher. |
| `EdgeTtsSynthesizer`, `PastedStorySource`, `EdgeBoundaryTranscriber` | Light tests. Straightforward wrappers — review skipped per requirements §9. |
| Compose stages | Integration test against real ffmpeg with synthetic B-roll. |

**Synthetic B-roll.** There is no library yet, so the exit criterion must not
depend on Anas sourcing footage first. Integration tests generate clips with
`ffmpeg -f lavfi -i testsrc2=...`, which is deterministic and costs nothing;
integration tests already shell out to a real ffmpeg. Real footage is for the
human "does it look good" check, not for the automated criterion.

### 10.1 Guard-pinning discipline

Carried forward unchanged, because it is the rule that has paid off every phase:
for any test whose name asserts a *reason*, the brief must state the expected
failure **message or reason**, and the implementer must delete the guard and
watch that test fail **for that reason**.

**If a predicted failure does not materialise, or materialises for a different
reason, report that rather than smoothing it over.** Where a guard is
unfalsifiable by deletion, prove non-vacuity by mutation and record the
exception in the code.

---

## 11. Task order

1. **Stderr-to-file + pump deadline.** Blocker on every ffmpeg stage.
2. Migration 004 + `projects` persistence.
3. Port widening (`Narration`, `WordBoundary`), settings plumbing, stage
   registry, fingerprint helper. One task — they are one seam.
4. `ingest_story` + `PastedStorySource`.
5. `synthesize_speech` + `EdgeTtsSynthesizer`.
6. `transcribe` + `EdgeBoundaryTranscriber`.
7. `plan_timeline` (pure).
8. `.ass` writer (pure, both canvases).
9. `ytauto broll add` — probe, dual normalise, row, manifest.
10. `select_broll` + `LibraryVisualStrategy`.
11. `compose_landscape`.
12. `compose_vertical`.
13. `ytauto run` — enqueue and drain.
14. Exit criteria as integration tests.

1–3 are infrastructure with no visible output; 4 onwards each add a stage that
can be run and inspected.

---

## 12. Risks

- **`edge-tts` is an unofficial client of a Microsoft endpoint.** It can break
  without notice, and it is the default path's only speech source. Mitigated by
  the port: Piper is a drop-in replacement, at the cost of needing Whisper. This
  is a real single point of failure for the "free" claim and should be
  acknowledged rather than designed around.
- **First real third-party runtime dependency.** Today the only one is
  `platformdirs`. `edge-tts` brings a transitive tree; it should be pinned.
- **Two normalised blobs per clip doubles library storage.** Acceptable at the
  scale of one user's library, and the eviction ceiling is not yet enforced
  because the Evictor is unwired.
- **Neither compose stage has ever run.** The nvenc pixel-format and filter-graph
  surface is entirely unexercised; §6's log file exists partly for this.
