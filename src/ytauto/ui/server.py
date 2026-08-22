"""Serving the UI: the one place a socket is opened.

Separate from ``ui.app`` so that constructing the application (which the
endpoint tests do, hundreds of times, against a temporary data directory) is
never entangled with binding a port.

Werkzeug's ``run_simple`` rather than ``Flask.run``, for one reason: it is
explicit about threading. ``threaded=True`` is what makes the polling endpoint
answerable while a page is mid-request, and it is also why every database
connection in ``ui.app`` is opened per request rather than shared. (It still
prints Werkzeug's own "this is a development server" warning. That warning is
correct and is left alone: this *is* a development server, deliberately, and
suppressing the one line that says so to make a personal tool look
production-grade would be the wrong trade.)
"""

from __future__ import annotations

from werkzeug.serving import run_simple

from ytauto.infra.paths import AppPaths
from ytauto.ui import DEFAULT_PORT, HOST
from ytauto.ui.app import create_app


def serve(paths: AppPaths, *, port: int = DEFAULT_PORT) -> None:
    """Serve the UI on loopback until interrupted.

    The listen address is ``ytauto.ui.HOST`` and is not a parameter. See that
    constant: this application has no authentication, and the only reason to
    make the bind address configurable would be to let someone remove the one
    thing protecting it.

    ``use_reloader=False`` deliberately: the reloader re-executes the process,
    which would abandon any render thread already running and leave its SQLite
    connection to be closed by nobody.

    Raises:
        OSError: the port is already in use, or cannot be bound.
        KeyboardInterrupt: Ctrl-C. Propagated rather than swallowed so the
            caller decides what a clean shutdown prints.
    """
    app = create_app(paths)
    try:
        run_simple(HOST, port, app, threaded=True, use_reloader=False, use_debugger=False)
    finally:
        tasks = app.config["YTAUTO_TASKS"]
        tasks.close()
