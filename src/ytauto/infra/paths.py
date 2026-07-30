"""Filesystem layout for everything the application writes.

No module outside this one computes an application path. That rule is what
lets the data directory be relocated, tested against ``tmp_path``, and later
redirected by a packaged installer without touching call sites.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from platformdirs import user_data_dir

from ytauto.core.errors import ConfigurationError

_ENV_VAR = "YTAUTO_DATA_DIR"


@dataclass(frozen=True)
class AppPaths:
    """Resolved, absolute locations for application data."""

    root: Path
    projects: Path
    cas: Path
    logs: Path
    cache: Path
    exports: Path
    db_file: Path

    @classmethod
    def resolve(cls, override: Path | None = None) -> AppPaths:
        """Resolve the data root: explicit override, then env var, then platform default.

        Computes paths only - nothing is created and nothing is written, so this
        succeeds even when the resulting root is unwritable. Call ensure() for
        that.

        Raises:
            RuntimeError: a path used '~' and the home directory could not be
                determined.
        """
        if override is not None:
            root = Path(override)
        elif from_env := os.environ.get(_ENV_VAR):
            root = Path(from_env)
        else:
            root = Path(user_data_dir(appname="ytauto", appauthor="ytauto"))

        root = root.expanduser().resolve()
        return cls(
            root=root,
            projects=root / "projects",
            cas=root / "assets" / "cas",
            logs=root / "logs",
            cache=root / "cache",
            exports=root / "exports",
            db_file=root / "ytauto.db",
        )

    def ensure(self) -> None:
        """Create every directory. Idempotent.

        Note what this does NOT guarantee: mkdir(parents=True, exist_ok=True)
        on an *existing* directory succeeds regardless of write permission, so
        returning cleanly does not mean the directories are writable. Callers
        that then open a file inside them must still handle OSError.

        Raises:
            ConfigurationError: a directory could not be created. An unwritable
                data root is a misconfiguration the user must resolve, not a
                transient fault - so it enters the typed taxonomy here rather
                than leaking a raw OSError to every caller.
        """
        for directory in (self.root, self.projects, self.cas, self.logs, self.cache, self.exports):
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise ConfigurationError(
                    f"cannot create application directory {directory}: {exc}"
                ) from exc
