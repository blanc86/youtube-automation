"""Filesystem layout for everything the application writes.

No module outside this one computes an application path. That rule is what
lets the data directory be relocated, tested against ``tmp_path``, and later
redirected by a packaged installer without touching call sites.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from pathlib import Path

from platformdirs import user_data_dir, user_downloads_dir, user_videos_dir

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


def ensure_writable_dir(directory: Path) -> bool:
    """Create ``directory`` (and any missing parents) and *prove* it is
    writable by actually writing and removing a probe file, rather than
    inferring it from permission bits.

    ``os.access(directory, os.W_OK)`` is not used here: on Windows it can
    report a directory writable when a real write still fails (ACL
    inheritance quirks, a cloud-synced folder mid-sync, a read-only
    filesystem mounted read-write in appearance). Doing the write for real is
    the only check that cannot lie.

    Returns ``False`` (never raises) for any ``OSError`` encountered while
    creating the directory or writing/removing the probe file - a caller
    deciding between a preferred location and a fallback wants a boolean, not
    an exception to catch. ``AppPaths.ensure()`` above is the analogous
    "create or raise" helper for the application's own internal data root;
    this one is "create and prove writable, or say no" for user-facing output
    locations, where a caller has a fallback to try next.
    """
    try:
        directory.mkdir(parents=True, exist_ok=True)
        probe = directory / f".ytauto-write-test-{uuid.uuid4().hex}"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
    except OSError:
        return False
    return True


def resolve_output_dir(
    *,
    videos_dir: Path | None = None,
    downloads_dir: Path | None = None,
) -> Path:
    """Resolve the directory rendered video masters are exported to so a
    person can actually find them - deliberately separate from
    ``AppPaths.exports``, which lives under the platform's *application data*
    directory (``%LOCALAPPDATA%`` on Windows) and is not somewhere anyone
    looks for their own files.

    Tries, in order:

    1. ``<user's Videos folder>/ytauto`` - ``platformdirs.user_videos_dir()``
       reads the real, possibly-redirected location (the Windows registry
       value, honouring a Videos folder moved to another drive or into
       OneDrive; ``~/Movies`` on macOS, not ``~/Videos``) rather than
       assuming a hardcoded path.
    2. ``<user's Downloads folder>/ytauto`` - if the videos location cannot
       be created or is not actually writable once created.

    ``videos_dir``/``downloads_dir`` override the platform-detected roots.
    This is what makes the function testable (a test can point ``videos_dir``
    at an unwritable path to drive the fallback branch without touching the
    real Videos folder) and injectable for a future caller - e.g. a Web UI -
    that wants a different root entirely.

    Raises:
        ConfigurationError: neither candidate could be created and proven
            writable. Both attempted paths are named in the message - the
            whole point of this function is that the user knows exactly
            where their video was supposed to go, so a third, silent,
            fallback location is never chosen on their behalf.
    """
    videos_root = Path(videos_dir) if videos_dir is not None else Path(user_videos_dir())
    downloads_root = (
        Path(downloads_dir) if downloads_dir is not None else Path(user_downloads_dir())
    )

    videos_candidate = videos_root / "ytauto"
    if ensure_writable_dir(videos_candidate):
        return videos_candidate

    downloads_candidate = downloads_root / "ytauto"
    if ensure_writable_dir(downloads_candidate):
        return downloads_candidate

    raise ConfigurationError(
        "could not find a writable location to save rendered videos - tried "
        f"{videos_candidate} and {downloads_candidate}. Pass --output-dir to "
        "ytauto run to choose a location explicitly."
    )
