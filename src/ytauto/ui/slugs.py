"""Deriving a project slug from a title, so nobody has to type one.

``ytauto project create`` requires ``--slug`` and ``--title`` separately.
That is fine at a command line, where the slug is also the handle you type
into ``ytauto run --project``; it is friction in a browser, where the user
has already named the thing once. So the UI asks for a title and derives the
slug.

The slug is not decorative. It is a URL path segment, a SQLite ``UNIQUE``
column, *and* a directory name under ``AppPaths.projects``. That third role
is what makes this more than ``lower().replace(" ", "-")``.
"""

from __future__ import annotations

import re
import sqlite3
import unicodedata

MAX_SLUG_LENGTH = 60
"""Long enough for any reasonable title, short enough that
``<data root>/projects/<slug>/story.txt`` stays well clear of Windows'
260-character path limit even under a deeply nested ``%LOCALAPPDATA%``."""

_FALLBACK = "project"
"""What a title with no usable characters at all becomes - an emoji-only
title, or one written entirely in a script ``unicodedata`` cannot fold to
ASCII. Producing an empty slug instead would create a project directory
named ``""``, which resolves to the projects root itself."""

_RESERVED_NAMES = frozenset(
    {
        "con",
        "prn",
        "aux",
        "nul",
        *(f"com{n}" for n in range(1, 10)),
        *(f"lpt{n}" for n in range(1, 10)),
    }
)
"""Windows device names. ``projects/nul/story.txt`` is not a file - the OS
resolves ``nul`` to the null device regardless of the directory it appears
in - so a project titled "Nul" would silently write its story nowhere and
read back an empty one. These get the same numeric suffix a collision does
rather than a different, special-cased shape."""

_NON_SLUG = re.compile(r"[^a-z0-9]+")


def slugify(title: str) -> str:
    """Fold ``title`` to a lowercase, hyphen-separated, ASCII-only slug.

    Accented characters are decomposed and stripped of their marks
    (``Café`` -> ``cafe``) rather than dropped wholesale, which is the
    difference between a recognisable slug and ``caf``. Everything else
    outside ``[a-z0-9]`` becomes a single hyphen, and leading/trailing
    hyphens are trimmed.

    Length is capped at ``MAX_SLUG_LENGTH`` and the cut is trimmed back to a
    hyphen boundary so a truncated slug does not end mid-word with a stray
    separator. Never returns an empty string; see ``_FALLBACK``.

    Pure: this does not know whether the result is already taken. That is
    ``unique_slug``'s job.
    """
    folded = unicodedata.normalize("NFKD", title)
    ascii_only = folded.encode("ascii", "ignore").decode("ascii")
    hyphenated = _NON_SLUG.sub("-", ascii_only.lower()).strip("-")
    if len(hyphenated) > MAX_SLUG_LENGTH:
        hyphenated = hyphenated[:MAX_SLUG_LENGTH].rstrip("-")
    return hyphenated or _FALLBACK


def unique_slug(conn: sqlite3.Connection, title: str) -> str:
    """``slugify(title)``, made unique against the ``projects`` table.

    On collision, appends ``-2``, then ``-3``, and so on: the second project
    called "The Ghost Train" gets ``the-ghost-train-2``. Counting from 2
    rather than 1 is deliberate - the unsuffixed slug *is* the first one, and
    a set reading ``the-ghost-train``/``the-ghost-train-1`` invites the
    question of which came first.

    A Windows device name (``nul``, ``com1``, ...) is treated as taken even
    though no project holds it, so it takes the same ``-2`` suffix - see
    ``_RESERVED_NAMES``.

    The suffix is applied to a base trimmed to leave room for it, so the
    result never exceeds ``MAX_SLUG_LENGTH`` and a very long title cannot
    make two different projects collide *again* by both being truncated back
    to the same string.

    This is advisory, not a lock: two browser tabs submitting the same title
    at the same instant can both read "free" here. ``ProjectService.create``
    holds the authoritative ``UNIQUE`` constraint and raises
    ``ValidationError`` on a genuine race, which the caller reports as a
    normal form error.
    """
    base = slugify(title)
    if not _taken(conn, base):
        return base
    suffix = 2
    while True:
        tail = f"-{suffix}"
        trimmed = base[: MAX_SLUG_LENGTH - len(tail)].rstrip("-") or _FALLBACK
        candidate = f"{trimmed}{tail}"
        if not _taken(conn, candidate):
            return candidate
        suffix += 1


def _taken(conn: sqlite3.Connection, slug: str) -> bool:
    if slug in _RESERVED_NAMES:
        return True
    row = conn.execute("SELECT 1 FROM projects WHERE slug = ? LIMIT 1", (slug,)).fetchone()
    return row is not None
