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
line — or from a local web UI (`ytauto ui`) — and produces watchable files.
There is no metadata or thumbnail generation and no auto-upload — see
[What's not built yet](#whats-not-built-yet).

---

## What you need

- **Python 3.12 or newer.** `python --version` should say 3.12+.
- **ffmpeg and ffprobe**, both on your `PATH`, in a build that includes
  **libass** (captions are burned in with it). Almost every general-purpose
  build has it; see below.
- **An internet connection** for speech synthesis. Nothing else phones home.
- **A GPU is optional.** `h264_nvenc` is used when present, falling back to
  `h264_qsv`, then `libx264`, automatically. No GPU just means a slower encode.

### Installing ffmpeg

**Windows** (either one):

```bash
winget install Gyan.FFmpeg
```

```bash
choco install ffmpeg-full
```

**macOS:**

```bash
brew install ffmpeg
```

**Linux (Debian/Ubuntu):**

```bash
sudo apt install ffmpeg
```

Close and reopen your terminal afterwards, then check both binaries are found:

```bash
ffmpeg -version
```

---

## Install

```bash
git clone https://github.com/blanc86/youtube-automation.git
cd youtube-automation
```

Create a virtual environment and **activate it** — every command below assumes
an activated environment, and this is the step most easily skipped:

**Windows (PowerShell):**

```bash
python -m venv .venv; .venv\Scripts\Activate.ps1
```

**Windows (cmd):**

```bash
python -m venv .venv && .venv\Scripts\activate.bat
```

**macOS / Linux:**

```bash
python3 -m venv .venv && source .venv/bin/activate
```

Your prompt should now start with `(.venv)`. Then install:

```bash
pip install -e ".[dev]"
```

Now confirm the whole environment is sane — ffmpeg, paths, disk, database:

```bash
ytauto doctor
```

If that prints no errors, you are ready. Every later session needs the
activation step again (the `pip install` is one-time).

> **Reinstall after pulling.** Pipeline stages are registered as Python entry
> points, so a stale editable install silently advertises fewer stages than the
> code has — and a job will report success having rendered nothing. If you pull
> and anything behaves oddly, run `pip install -e ".[dev]"` again first. This
> has been the cause of more confusing behaviour than any actual bug.

---

## The five-minute version

From a clean clone to a finished video, with the web UI:

```bash
ytauto broll add path/to/any-video.mp4 --source-url https://example.com --licence CC0
```

```bash
ytauto ui
```

Open **http://127.0.0.1:8765**, click **New project**, paste a story (or copy
the prompt on that page into any chat assistant and paste its reply), then
press **Render**. The finished folder is shown on screen when it's done.

Everything past this point is detail.

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
yourself, or paste something you generated elsewhere.

It's copied into the project directory as the human-readable source of truth, so
you can reopen and revise it later.

**Don't want to write it yourself?** [`docs/SCRIPT-PROMPT.md`](docs/SCRIPT-PROMPT.md)
has a ready-made prompt — paste it into any chat assistant with your plot idea and
the reply is already in the exact shape this expects. No API key, no cost.

### 3. Render

```bash
ytauto run --project ghost-train
```

Seven stages run in order, each in its own worker process. When it finishes you
have both masters. Exit code is `0` on success, `1` if the job failed, `2` for
bad input.

---

## Or skip the command line

```bash
ytauto ui
```

Then open **http://127.0.0.1:8765**. Everything above is there: create a
project by pasting a story (the slug is derived from the title), edit that
story later, change settings including caption colours, add B-roll and
music, and render — with the output folder shown when it finishes.

It binds to loopback only and has no authentication, deliberately: it is a
tool for one person on one machine. There is no `--host` flag; `--port`
changes the port.

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
| `music_track_id` | `""` | A track from the music library; empty means no bed |
| `music_gain_db` | `-18.0` | The bed's level. Applied to the music alone |

### Music

Optional, and off by default — a video with no bed is a finished video.

```bash
ytauto music add C:\music\slow-pulse.mp3 --source-url https://example.com/track --licence CC0
```

Then pick it on the project page and set its level. The volume is independent
of the narration: the gain applies to the bed alone, so the voice keeps its
level whatever you do. A track shorter than the video loops, and every bed
fades out at the end.

**The source URL and licence are required, and this matters more here than it
does for footage.** Music is the most Content-ID-matched category on YouTube,
matching runs automatically on every upload, and a claim on the bed takes the
whole video however well-licensed the footage under it is.

> **There is no `project set-setting` command yet.** To change a setting from
> the CLI today you edit the project's `settings_json` in the SQLite database
> directly. The web UI (`ytauto ui`) has a form for all of them, which is
> currently the only comfortable way to do it.

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
    --output-dir PATH         where to write the masters
ytauto music add <path>       ingest a music track
    --source-url URL          where it came from        (required)
    --licence TEXT            its licence               (required)
    --title TEXT              display name (defaults to the filename)
    --attribution TEXT        attribution, if needed
    --notes TEXT              free-form notes
ytauto music list             list every track in the library
ytauto ui                     serve the local web UI on 127.0.0.1
    --port N                  port to listen on         (default 8765)

ytauto --data-dir PATH        use a different data directory
ytauto --version
```

---

## What's not built yet

Deliberately out of scope for this phase, in rough order of likely arrival:

- **Metadata and thumbnails** — title, description, hashtags, cover image as a
  real pipeline stage.
- **`project set-setting`**, and a `broll list` — both exist in the web UI.
- **AI-generated video** as an alternative to your own footage.
- **Auto-upload.** Chosen against deliberately: YouTube's API allows about six
  uploads a day against a target of 20–100. The pipeline will prepare everything
  and hand off; you post.

### If something goes wrong

| What you see | What it is |
|---|---|
| `ytauto: command not found` | The virtual environment isn't activated. Re-run the activate step from [Install](#install). |
| `ModuleNotFoundError` for a package you know is installed | You're on a different Python than the one you installed into — activate the venv, or re-run `pip install -e ".[dev]"`. |
| A job succeeds but no video appears | Almost always a stale editable install. Run `pip install -e ".[dev]"` again. |
| `has no 'ass' filter (libass)` | Your ffmpeg build can't burn captions. Install one of the builds listed under [Installing ffmpeg](#installing-ffmpeg). |
| `no h264 encoder` | Same cause, same fix. |
| Renders finish instantly and nothing changes | That's the cache doing its job. Change a setting, or edit the story, and it re-runs the stages that actually depend on it. |

`ytauto doctor` checks most of this in one command, and is the first thing to
run when anything behaves oddly.

### Known rough edges

- **Trailing silence is trimmed.** The video ends where the last word ends, so a
  beat of breathing room at the end is lost.
- **One bad job can block `ytauto run`** for an unrelated project until its lease
  expires.

---

## Development

The quality gate runs everything CI does:

```bash
python scripts/check.py
```

That's ruff, mypy, import-linter and both test suites — 687 unit tests and 22
integration tests. The integration suite drives real ffmpeg and real speech
synthesis, so it needs a network connection and takes a couple of minutes.

Architecture, extension points and contributor tasks:

- [`docs/EXTENDING.md`](docs/EXTENDING.md) — how to add a provider
- [`docs/CONTRIBUTOR-TASKS.md`](docs/CONTRIBUTOR-TASKS.md) — ready-to-take work
- [`docs/superpowers/specs/`](docs/superpowers/specs/) — design documents

**Adding a stage or provider means adding an entry point**, which means everyone
must reinstall. That's the one piece of friction worth knowing about before you
start.
