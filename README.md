# ytauto

Turns a written story into finished, narrated, word-captioned videos — landscape
**and** vertical — on your own machine, for nothing.

Paste a story in. Get back two rendered MP4s: 1920×1080 for long-form and
1080×1920 for Shorts, narrated, cut over your own B-roll, with the spoken word
highlighted as it's said.

**Free on the default path.** Not cheap — free. Your story, Microsoft Edge's
free voices, your own footage, ffmpeg on your own GPU. No API keys, no per-video
cost. Paid providers exist only as opt-in upgrades behind the same interfaces.

**Status:** Phase 2a complete. The pipeline runs end to end from the command
line and produces watchable files. There is no web UI yet, no metadata or
thumbnail generation, and no auto-upload — see [What's not built yet](#whats-not-built-yet).

---

## What you need

- **Python 3.12+**
- **ffmpeg and ffprobe** on your `PATH` (7.x recommended)
- **A GPU is optional.** `h264_nvenc` is used when present; it falls back to
  `h264_qsv`, then `libx264`, automatically.
- **An internet connection** for speech synthesis. Nothing else phones home.

## Install

```bash
git clone https://github.com/blanc86/youtube-automation.git
cd youtube-automation
python -m venv .venv
```

Windows:

```bash
.venv\Scripts\pip install -e ".[dev]"
```

macOS / Linux:

```bash
.venv/bin/pip install -e ".[dev]"
```

Then check your environment:

```bash
ytauto doctor
```

> **Reinstall after pulling.** Pipeline stages are registered as Python entry
> points, so a stale editable install silently advertises fewer stages than the
> code has — and a job will report success having rendered nothing. If you pull
> and anything behaves oddly, run `pip install -e ".[dev]"` again first.

---

## Make your first video

Three commands. About a minute, most of it ffmpeg.

### 1. Add some B-roll

The library is what fills the picture behind your narration. Every clip records
where it came from and under what licence — that record is your DMCA defence, so
both flags are required.

```bash
ytauto broll add C:\clips\dark-forest-drive.mp4 --source-url https://example.com/clip --licence CC0
```

Each clip is transcoded **twice** — once for each canvas, both from the original,
so neither format is a stretched crop of the other. Add three or four clips
before your first render; the selector avoids repeats until the library runs out.

### 2. Create a project

```bash
ytauto project create --slug ghost-train --title "The Ghost Train" --story story.txt
```

`story.txt` is a plain UTF-8 text file — whatever you want narrated. Write it
yourself, or paste something you generated elsewhere. It's copied into the
project directory as the human-readable source of truth, so you can reopen and
revise it later.

### 3. Render

```bash
ytauto run --project ghost-train
```

Seven stages run in order, each in its own worker process. When it finishes you
have both masters. Exit code is `0` on success, `1` if the job failed, `2` for
bad input.

---

## What actually happens

| # | Stage | What it does |
|---|---|---|
| 1 | `ingest_story` | Reads your story file into the pipeline |
| 2 | `synthesize_speech` | Narrates it with Edge voices — **and captures when each word is spoken** |
| 3 | `transcribe` | Turns those word boundaries into a timing list. No speech recognition, no GPU |
| 4 | `plan_timeline` | Decides the edit: which words group into each caption, where the footage cuts |
| 5 | `select_broll` | Picks a clip for each gap, seeded so the same story always cuts the same way |
| 6 | `compose_landscape` | One ffmpeg pass: stitch, burn captions, mux narration, encode → 1920×1080 |
| 7 | `compose_vertical` | The same at 1080×1920, from identical upstream work |

**The word-timing trick is why this is free.** Edge hands back the exact moment
every word is spoken, so speech recognition never runs — which is what keeps the
default path off the GPU entirely until the final encode.

### Work is never done twice

Every stage computes a fingerprint of its inputs, settings and version. If output
already exists under that fingerprint, the stage is skipped.

- Change the caption colour → only the two encodes re-run.
- Change the voice → narration onward re-runs; your story isn't re-read.
- Change nothing → a re-run does no work at all.
- Edit your story → everything re-runs, correctly.

### A crash costs you one stage

Jobs live in a queue. If a worker dies — a bad file, a hung encoder, a power cut
— only that stage is lost. Run the command again and it resumes from the last
completed stage.

---

## Tuning a render

Settings live per project. `project create` seeds working defaults:

| Setting | Default | What it does |
|---|---|---|
| `voice` | `en-US-AriaNeural` | Any Edge voice name |
| `rate` | `+0%` | Speech rate, e.g. `+10%` |
| `seed` | `1` | Change for a different cut of the same story |
| `words_per_group_min` | `3` | Advisory minimum words per caption |
| `words_per_group_max` | `5` | Hard maximum before a caption breaks |
| `segment_seconds_min` | `1.5` | Shortest B-roll segment |
| `segment_seconds_max` | `4.0` | Longest B-roll segment |
| `caption_style` | `{}` | Font, size, colours; empty means all defaults |
| `encoder` | `auto` | Or name one explicitly, e.g. `libx264` |

> **There is no `project set-setting` command yet.** To change a setting today
> you edit the project's `settings_json` in the SQLite database directly. That's
> the most visible rough edge in the current CLI.

---

## Every command

```
ytauto doctor                 check ffmpeg, paths, disk and database
ytauto broll add <path>       ingest a clip into the library
    --source-url URL          where it came from        (required)
    --licence TEXT            its licence               (required)
    --attribution TEXT        attribution, if needed
    --notes TEXT              free-form notes
ytauto project create         create a project from a story file
    --slug SLUG               url-safe unique id        (required)
    --title TITLE             human-readable title      (required)
    --story PATH              path to the story         (required)
ytauto run                    render one project
    --project SLUG            which project             (required)
    --max-ticks N             dispatcher budget per poll round (default 100)

ytauto --data-dir PATH        use a different data directory
ytauto --version
```

---

## What's not built yet

Deliberately out of scope for this phase, in rough order of likely arrival:

- **A local web UI.** The engine has been kept free of any UI framework so this
  is a deployment decision, not a rewrite.
- **Metadata and thumbnails** — title, description, hashtags, cover image as a
  real pipeline stage.
- **`project set-setting`**, and a `broll list`.
- **AI-generated video** as an alternative to your own footage.
- **Auto-upload.** Chosen against deliberately: YouTube's API allows about six
  uploads a day against a target of 20–100. The pipeline will prepare everything
  and hand off; you post.

### Known rough edges

- **Trailing silence is trimmed.** The video ends where the last word ends, so a
  beat of breathing room at the end is lost.
- **Captions blink at sentence boundaries** — roughly a 0.85 s gap where nothing
  is on screen, because a caption group ends on the last word rather than
  reaching the next one.
- **One bad job can block `ytauto run`** for an unrelated project until its lease
  expires.

---

## Development

The quality gate runs everything CI does:

```bash
python scripts/check.py
```

That's ruff, mypy, import-linter and both test suites — 625 unit tests and 22
integration tests. The integration suite drives real ffmpeg and real speech
synthesis, so it needs a network connection and takes a couple of minutes.

Architecture, extension points and contributor tasks:

- [`docs/EXTENDING.md`](docs/EXTENDING.md) — how to add a provider
- [`docs/CONTRIBUTOR-TASKS.md`](docs/CONTRIBUTOR-TASKS.md) — ready-to-take work
- [`docs/superpowers/specs/`](docs/superpowers/specs/) — design documents

**Adding a stage or provider means adding an entry point**, which means everyone
must reinstall. That's the one piece of friction worth knowing about before you
start.
