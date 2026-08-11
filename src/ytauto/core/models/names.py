"""One duplicate-name check, shared by everything that needs it.

Three separate implementations existed before this, two of them quadratic and
all three with different message shapes, so the same violation read differently
depending on which layer caught it.
"""

from __future__ import annotations

from collections.abc import Iterable

from ytauto.core.errors import ValidationError


def assert_unique_names(names: Iterable[str], *, what: str, context: str) -> None:
    """Raise if any name repeats.

    Args:
        names: the names to check, in order.
        what: singular noun for the thing being named, e.g. ``"stage"``.
        context: where the collision happened, e.g. ``"pipeline 'intro'"``.

    Raises:
        ValidationError: if a name appears more than once. The message names the
            first repeat in iteration order, so it is deterministic.
    """
    seen: set[str] = set()
    for name in names:
        if name in seen:
            raise ValidationError(f"duplicate {what} name in {context}: {name!r}")
        seen.add(name)
