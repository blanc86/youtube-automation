"""Reading ``docs/SCRIPT-PROMPT.md`` at runtime for the "Need a script?" panel.

The prompt is not duplicated into this package. It is one of this project's
actual deliverables - measured against real renders, with a note in its own
Notes section explaining why the word counts are what they are - and a second
copy in a template would be wrong within one edit of the first.

So the file is read when the page is served. That has one consequence worth
being honest about: an installed-but-not-editable copy of ``ytauto`` has no
``docs/`` directory beside it, and the panel then says so rather than showing
a stale hardcoded copy. Everything else on the page still works; this is a
convenience panel, not a dependency of creating a project.
"""

from __future__ import annotations

import os
from pathlib import Path

ENV_VAR = "YTAUTO_SCRIPT_PROMPT"
"""Overrides the search below with an explicit path. Exists for the packaged
case (and for tests, which point it at a fixture rather than reaching for the
repository's own docs from an arbitrary working directory)."""

_FENCE = "```"
_PROMPT_HEADING = "## The prompt"

MISSING_MESSAGE = (
    "docs/SCRIPT-PROMPT.md could not be found next to this installation. "
    "It is in the repository - open it there, or set the YTAUTO_SCRIPT_PROMPT "
    "environment variable to its path."
)


def candidate_paths() -> tuple[Path, ...]:
    """Where to look, in order.

    The ``parents[3]`` entry is the editable-install case and the only one
    that normally hits: this module is ``<repo>/src/ytauto/ui/script_prompt.py``,
    so three levels up from its directory is the repository root. The
    working-directory entry covers someone running ``ytauto ui`` from a
    checkout that is not the one they installed from.
    """
    override = os.environ.get(ENV_VAR)
    if override:
        return (Path(override),)
    here = Path(__file__).resolve()
    return (
        here.parents[3] / "docs" / "SCRIPT-PROMPT.md",
        Path.cwd() / "docs" / "SCRIPT-PROMPT.md",
    )


def load() -> str:
    """The prompt text to show, or ``MISSING_MESSAGE``.

    Never raises. An unreadable docs file must not take down the page that
    happens to link to it.
    """
    for candidate in candidate_paths():
        try:
            if candidate.is_file():
                return extract(candidate.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError):
            continue
    return MISSING_MESSAGE


def extract(markdown: str) -> str:
    """The fenced block under ``## The prompt``, or the whole document.

    The document is prose *about* the prompt wrapped around the prompt
    itself; pasting the prose into a chat assistant would confuse it. So this
    finds the heading, then the first fenced block after it, and returns its
    contents.

    Falls back to the entire document if that shape is not found - a
    rewritten SCRIPT-PROMPT.md should degrade to "shows too much" rather than
    "shows nothing", since the user can still read it and take what they
    need.
    """
    lines = markdown.splitlines()
    try:
        start = next(i for i, line in enumerate(lines) if line.strip() == _PROMPT_HEADING)
    except StopIteration:
        return markdown.strip()
    body: list[str] | None = None
    for index in range(start + 1, len(lines)):
        if lines[index].startswith(_FENCE):
            body = []
            for line in lines[index + 1 :]:
                if line.startswith(_FENCE):
                    return "\n".join(body).strip()
                body.append(line)
            break
    return markdown.strip() if body is None else "\n".join(body).strip()
