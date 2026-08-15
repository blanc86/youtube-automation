"""Reusable synthetic stages shared across integration tests that need a real
worker subprocess to run something specific, rather than a bespoke stage
declared in the test module itself.

Constructed by the ``ytauto.stages`` entry points declared in
``tests/ytauto_it_stages-0.0.0.dist-info/entry_points.txt`` - the same
mechanism a real provider package will ship with, and the same one
``app/registry.py`` resolves in both this process and the worker subprocess
it spawns. Two consequences:

1. A stage receives its ``CasStore`` from its factory, so writing its output
   no longer needs the CAS root smuggled through an environment variable, and
   no longer needs a throwaway ``:memory:`` connection of its own.
2. The worker subprocess must still be able to import this module.
   ``tests/`` reaches its ``sys.path`` through ``PYTHONPATH``, which the
   fixtures set and ``Popen`` inherits - see
   ``tests/integration/test_resume.py``'s module docstring.
"""

from __future__ import annotations

import sys
from collections.abc import Mapping

from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.infra.cas.store import CasStore


class StderrFlooder:
    """Writes 200 KB to stderr, then produces one artifact.

    Stands in for the ffmpeg stages Phase 2 adds: ffmpeg routinely logs far
    more than the 60 KB measured to deadlock the pre-fix pump (see
    ``dispatcher._pump``), and clears it in seconds under normal use.
    """

    id = "flood"
    version = 1
    depends_on: tuple[str, ...] = ()
    settings_keys: tuple[str, ...] = ()

    def __init__(self, cas: CasStore) -> None:
        self._cas = cas

    def fingerprint(self, ctx: JobContext) -> str:
        return "f" * 64

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        sys.stderr.write("x" * 200_000)
        sys.stderr.flush()
        digest = self._cas.stage_file(b"done", kind="text")
        return StageResult(artifacts=(ArtifactRef(name="out", kind="text", digest=digest),))


def make_stderr_flooder(*, cas: CasStore, settings: Mapping[str, object]) -> StderrFlooder:
    """Entry point ``it-flood:flood``."""
    return StderrFlooder(cas)
