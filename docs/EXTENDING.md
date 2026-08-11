# Extending ytauto

How to add a feature without touching anyone else's work — and without waiting
for the current phase to finish.

## The short version

Features plug in at **ports**: `Protocol` classes in
`src/ytauto/core/ports/providers.py` that define what a capability does without
saying how. An implementation lives in its own file under `src/ytauto/providers/`
and is discovered rather than hardcoded.

```
core/ports/providers.py     the seam    "a SpeechSynthesizer turns text into audio"
providers/piper.py          your code   "...here is one that uses Piper"
```

Because a provider is a new file behind an existing interface, two people can add
two providers at the same time and never touch the same line.

## Adding a provider behind an existing port

1. Read the port's `Protocol` in `core/ports/providers.py`. There are eight:
   `StorySource`, `ScriptGenerator`, `SpeechSynthesizer`, `Transcriber`,
   `VisualStrategy`, `ImageGenerator`, `ThumbnailRenderer`, `Publisher`.
2. Create `src/ytauto/providers/<name>.py` with a class implementing it.
3. Declare a `CapabilityDescriptor` (see `core/ports/capability.py`). This is not
   decoration — the scheduler reads `requires_gpu` and `vram_mb` to decide what
   may run concurrently, and `quality_tier` and cost fields drive provider
   selection. A wrong descriptor produces real scheduling bugs.
4. Test it without hitting the network. The protocols are `@runtime_checkable`,
   so `assert isinstance(MyProvider(), SpeechSynthesizer)` is a real conformance
   check.

Nothing else in the codebase needs to change.

## Adding a *new* port

Do this when the capability genuinely does not fit an existing seam — not when an
existing one is merely awkward.

1. Add the `Protocol` to `core/ports/providers.py`, `@runtime_checkable`, with a
   `Raises:` section on every method saying what an implementation is allowed to
   throw. That contract is the whole point: without it, three implementations
   will invent three different error conventions. This project already lived
   through that once and it produced a diagnostic tool that crashed on broken
   environments.
2. Remember `core/` imports **only** the standard library. A port that needs
   `numpy` in its signature is not a port.
3. Add the port to the pipeline that consumes it, as a `Stage`.

## Adding a pipeline stage

A `Stage` (see `core/pipeline/stage.py`) declares an `id`, a `version`, what it
`depends_on`, how to compute its `fingerprint`, and a `run`. The DAG in
`core/pipeline/graph.py` validates it, orders it, and answers what is downstream
of a change.

Two rules that are not obvious:

- **`version` is part of the fingerprint.** Bump it when your stage's output
  would change for the same inputs, or every cached artifact silently becomes
  wrong.
- **Name your outputs so they sort correctly.** `StageResult` sorts artifacts by
  name, because the cached path returns them name-ordered and the two must agree.
  If order matters for you — concatenating clips, say — encode it in the names:
  `seg_000`, `seg_001`.

## What you get for free

Anything you build inherits:

- **Content-addressed storage** with deduplication and LRU eviction.
- **Fingerprint caching** — a stage whose inputs, settings and version are
  unchanged is skipped. This is what makes iteration cheap and crash-resume work.
- **Crash resume** — a job killed mid-flight restarts at its last completed
  stage.
- **Structured logging** with a per-job correlation ID.
- **`ytauto doctor`** — add a check if your feature has an environment
  prerequisite.

## What is not built yet

`app/registry.py` — provider resolution with entry-point discovery — is specified
in the design (§5.4) but does not exist. Until it does, wiring a provider in
means adding it to a table rather than declaring it in your package metadata. See
the registry task in `docs/CONTRIBUTOR-TASKS.md`; it is the single change that
would make this project genuinely pluggable by third parties.

## Before you start

Check `docs/CONTRIBUTOR-TASKS.md` for the list of files currently being rewritten
by work in flight, and claim your task by commenting on its issue.
