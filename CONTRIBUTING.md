# Contributing

Read this before your first change. Most of it is short; the last section is the
part that actually matters.

## Workflow

`master` is protected. Nothing lands on it directly.

```bash
git checkout master
git pull
git checkout -b <phase-or-feature-name>
# work, commit as you go
python scripts/check.py          # must pass, on any platform
git push -u origin <your-branch>
```

Then open a pull request. CI runs the same `scripts/check.ps1` you just ran, on
a Windows runner, so a green local gate should mean a green PR.

Commit messages use `type: imperative summary` — `feat:`, `fix:`, `docs:`,
`test:`, `refactor:`. Explain **why** in the body when the reason is not
obvious from the diff. Several commits here are worth reading as examples.

## The quality gate

`python scripts/check.py` runs, in order: ruff, ruff format, mypy `--strict`,
import-linter, pytest unit, pytest integration. All of it must pass. It is one
file, shared by Windows, macOS, Linux and CI — `check.ps1` and `check.sh` are
three-line wrappers around it, and CI calls it directly.

That is deliberate. CI used to list the steps itself and drifted from the local
script, losing `ruff format --check` — so unformatted code could reach `master`.
One copy, no drift.

Run `python -m ruff format src tests` before the gate — formatting is checked,
not applied.

Two things encoded in that script are worth knowing before you touch it. For
eight tasks in Phase 0 the gate printed `ALL CHECKS PASSED` while running only
import-linter, because `$ErrorActionPreference` does not apply to native exit
codes in PowerShell 5.1. The fix attempt then used `python -m importlinter.cli`,
which exits 0 printing nothing because that module has no `__main__` guard — a
second silently-passing gate, caught only because someone tried to make it fail.

**If you add a step, demonstrate it failing before you trust it.**

## Working on macOS or Linux

Everything except the eventual GPU encode path is cross-platform, and CI runs
Windows and macOS on every push. Use `.venv/bin/...` where the docs say
`.venv\Scripts\...`, and `python scripts/check.py` for the gate.

`ytauto doctor` will report `libx264` rather than `h264_nvenc` for the encoder
and no NVIDIA GPU. That is expected and not an error — the render pipeline is
designed with that fallback. If `doctor` is otherwise green you have a working
development environment.

## Architecture rules the build enforces

- `core/` imports only the standard library. No `infra`, no `app`, no
  third-party packages.
- Nothing below `ui/` imports Qt.
- Layering is `ui → app → core`.
- `ytauto.core.*` passes `mypy --strict`.
- Migrations are append-only. Never edit a released migration; add a new one.
- Every public function in `core/`, `infra/` and `app/` carries a `Raises:`
  docstring section naming concrete exception types and *when* — or says nothing
  if it genuinely raises nothing.

## How work is planned

Larger changes go spec → plan → implementation:

1. A design doc in `docs/superpowers/specs/` settles the decisions and says why.
2. A plan in `docs/superpowers/plans/` breaks it into tasks, each with its own
   tests and its own commit.
3. Tasks get implemented and reviewed one at a time.

If you're picking up existing work, the plan tells you what's left. You don't
have to work this way for a small fix — but for anything touching the pipeline,
the schema, or the scheduler, the plan is where the reasoning lives.

`.superpowers/` is git-ignored working state. Anything worth keeping goes in
`docs/superpowers/`.

## Testing — the part that matters

This project has been bitten by the same defect twelve times, and it is not the
one you'd expect. Across two phases and twelve fix rounds, **every single
finding traced to a defect in the plan, not to an implementer's mistake.** The
dominant failure is a test that passes for the wrong reason.

Some real examples from this repo:

- A test asserting artifacts come back in name order passed with the `ORDER BY`
  clause deleted, because the primary key happened to supply the same order.
- A test named "schema version is part of the payload" never called the function
  that builds the payload.
- A suite stayed green with an entire transaction wrapper removed.
- A test pinning topological sort tie-breaking used a fixture that was a linear
  chain, which has only one valid order regardless of tie-breaking.

So, two rules:

**When a test exists to pin a guard** — a `try/except`, a validation branch, a
transaction wrapper, an `ORDER BY` — delete the guard, watch the test fail, put
it back. Not "watch something fail": watch *that* test fail *for that reason*.
"Confirm the test fails" is not good enough, because any failure satisfies it;
one of ours failed on an unrelated `IntegrityError` and let a wrong explanation
into three docstrings.

**When something doesn't behave the way you expected, say so.** Three times in
Phase 1a an implementer was told to expect a specific failure, saw something
different, and investigated instead of moving on — and each time found a real
defect that every review had missed. That habit is worth more than a clean-
looking result. If a predicted failure doesn't materialise, or materialises for
a different reason, put that in the PR description.

Occasionally a guard genuinely cannot be falsified by deletion. `ORDER BY name`
is one: the primary key supplies that order unconditionally, so no test can fail
without it. When that happens, prove the test isn't vacuous some other way
(mutate the clause instead of removing it), and record the exception in a
comment. An honest recorded exception beats a proof that doesn't prove anything.

## Getting help

`ytauto doctor` first — it diagnoses most environment problems by itself and
exits non-zero with a reason. If it's green and something still doesn't work,
open an issue with the `doctor` output in it.
