"""Pipeline stages that depend only on ``core`` ports, never on a concrete provider.

A stage here is typed against the Protocol under ``core/ports/providers.py``
that exists for exactly this seam - never against a class from
``ytauto.providers`` (a forbidden import - see ``pyproject.toml``'s
import-linter contracts). The concrete provider is constructed and injected
by the matching entry-point factory, which lives in ``providers/`` and is
therefore free to import both sides.
"""
