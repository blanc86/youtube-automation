"""The local web UI: a second front end onto exactly the same operations as the CLI.

Nothing in here reimplements pipeline behaviour. Creating a project is
``app.services.enqueue.create_project``; rendering is
``app.services.render.render_project`` - the same function ``ytauto run``
calls; adding a clip is ``infra.broll.BrollLibrary``. This package is forms,
tables, a stylesheet, and the small amount of glue that turns an HTTP request
into one of those calls.

``HOST`` and ``DEFAULT_PORT`` live at package level, away from anything that
imports Flask, so ``ytauto``'s argument parser can name the default port
without paying the framework's import cost on every ``ytauto doctor``.
"""

from __future__ import annotations

HOST = "127.0.0.1"
"""Loopback, and not configurable.

This UI has no authentication of any kind. It creates projects, rewrites
story files, adds B-roll and starts renders - every operation the CLI can
perform - for whoever can reach the socket. A ``--host`` flag would exist
only to be set to ``0.0.0.0`` by someone who wanted to open it on their
phone, which is why there is not one. Binding here means the OS refuses
connections from off this machine; nothing downstream has to get an
authorisation check right.
"""

DEFAULT_PORT = 8765
"""An arbitrary high port, chosen for being unlikely to collide with a dev
server someone already has running. ``ytauto ui --port`` changes it."""

__all__ = ["DEFAULT_PORT", "HOST"]
