# Phase 2 requirements — decided 2026-08-12

Captured from a scoping conversation that materially changed the product
definition. This supersedes conflicting statements in the original design spec,
and is the input to the Phase 2 design doc.

---

## 1. Who this is for

**Anas alone, running it on his own machine.** Not a product, not multi-user, not
yet.

He does expect paying customers *eventually*, and asked whether to hedge for it
now. **Decision: no.** No tenancy column, no owner ids, no accounts, no billing,
no metering. With one user and disposable development data, retrofitting that
later is a cheap migration — the earlier claim that it would be painful was
overstated for a solo-user database.

The reference product he wants to end up resembling is **Crayo AI** — but for its
*feature set*, not its business model. Crayo is a metered multi-tenant web SaaS
that edits video you upload; this is a personal generator that builds video from
a story. Overlap is roughly one of Crayo's fifteen-plus templates.

## 2. The binding constraint: it must be free to run

Not "cheap" — **free**, on the default path. This is the constraint that decides
every provider choice below.

Paid options must exist as opt-in upgrades behind the existing ports, never on
the default path.

### The free default stack

| Stage | Default | Cost |
|---|---|---|
| Story | **Pasted or written by hand** (see §3), or a cheap cloud LLM with his own key | £0, or fractions of a cent |
| Speech | `edge-tts` | free, no API key |
| Word timings | Edge `WordBoundary` events (spec §6.2) | free, **no GPU** |
| Background | His own B-roll library | free |
| Captions | libass — confirmed present by `doctor` | free |
| Render | ffmpeg + `h264_nvenc` on an RTX 3050 | free |

**The consequence worth carrying:** on the default path nothing takes a
`gpu_compute` lease. The 4 GB VRAM contention the governor exists to arbitrate
only binds once Whisper enters the picture, which only happens with Piper,
ElevenLabs or imported audio. Do not design Phase 2 as though the GPU is the
bottleneck on the free path — it is not involved.

### Opt-in upgrades, all behind existing ports

- **ElevenLabs** or another paid TTS. Note this *forces* `FasterWhisperTranscriber`
  for word timings, which is the point at which the GPU lease starts mattering.
- **Piper** — local, free, better than `edge-tts` in quality, but same Whisper
  consequence since it emits no word boundaries.
- **Cloud compute offload.** Already architecturally free: every provider sits
  behind a port, so a cloud implementation is a new file, not a redesign.
  **Nothing needs building now to keep this door open.**

## 3. Story input — three ways in, all first-class

Explicitly requested, and it is what makes a video cost literally nothing:

1. **Paste or type a story directly.** He may generate one in Gemini's web UI for
   free and paste it in. This must bypass both the fetcher and the LLM — not be
   bolted on afterwards.
2. **Generate with an LLM**, his own API key.
3. **Fetch from a source** (Reddit and similar), the original design's default.

`StorySource` already exists as a port, so (1) is a small implementation.

Genres are driven by his prompt — horror, relationship drama, and so on. Not
hardcoded categories.

## 4. Visuals

**B-roll first**, and it is the only visual path Phase 2 builds: gameplay
footage, driving through a dark forest, and similar. Normalised once at ingest to
the canvas spec, stream-copied thereafter (design §3.6).

**AI-generated video** — the look of channels like `@Blackfiles-HD` — is wanted
but **deferred**. It becomes a second provider behind the same seam once the
B-roll path produces watchable output. Rejected for now purely on cost: dollars
per clip against a near-zero budget.

**Worth doing cheaply now:** record provenance and licence per clip in the B-roll
library. This is not a SaaS hedge — it protects his own channel from DMCA, and
it is one column now versus auditing thousands of clips later.

## 5. Output formats

Both **long-form** and **Shorts/vertical**, sharing all upstream artifacts and
differing only in canvas, crop and a second encode pass. Already the design of
record; unchanged.

## 6. Publishing — prepare, don't auto-upload

**Chosen deliberately over API automation, because the arithmetic does not work.**
YouTube's Data API allows 10,000 quota units a day at 1,600 per upload —
**six uploads per day**, hard cap, against a target of 20–100 rendered. Instagram
and TikTok add Business-account and app-review friction.

So the pipeline ends by producing, alongside the video:

- title, description, hashtags
- a thumbnail
- whatever else raises the odds of the video travelling

…and then hands off. He reviews and posts. No quota ceiling, no ToS risk,
identical treatment for YouTube, Instagram and TikTok.

This means **metadata generation is a real pipeline stage**, not an afterthought.

### Where n8n fits

He explicitly offered n8n. The honest split:

- **n8n owns distribution.** The app renders and writes a manifest; n8n picks it
  up and handles the platform-specific work. New platforms become dragged nodes
  rather than Python edits — and this is the layer that changes most often.
- **Python owns making the video.** ffmpeg passes, caption timing,
  content-addressed caching, crash-resume mid-render. n8n has no good answer for
  any of it.

## 7. UI

**A local web UI**, served on `localhost`. Free to run, which was his only
condition.

**Not PySide6.** This supersedes the original spec's Qt desktop app. The switch
is nearly free *because* he required the engine core stay Qt-free in session one —
`import-linter` has enforced it on every commit since. If this ever becomes a
product, "productise it" is then a deployment rather than a frontend rewrite.

## 8. Extensibility — the reason for the architecture

Modularity was his original motivation and it must keep paying off. The live test
case: **auto-clipping**, already written up as task 11 in
`docs/CONTRIBUTOR-TASKS.md`, to be built by a friend and merged without touching
the core.

One architectural note that follows: clipping takes **existing video** as input,
whereas the pipeline currently assumes text. Phase 2 should not close the door on
a second entry point.

## 9. Process

He has twice flagged that this is taking too long, and he is right.

For Phase 2: reserve per-task review for where a bug is **expensive and silent** —
the render path and anything touching the cache or fingerprints. Skip it for
straightforward provider adapters. The rigour earned its place on the scheduler,
where a wrong guess corrupts state overnight; it is overkill for an `edge-tts`
wrapper.

## 10. Explicitly not in Phase 2

- Tenancy, accounts, billing, metering
- API auto-upload to any platform
- AI-generated video
- Auto-clipping (a contributor issue, not core work)
- PySide6
