"""Reusable synthetic stages shared across integration tests that need a real
worker subprocess to run something specific, rather than a bespoke stage
declared in the test module itself.

Zero-arg constructible and importable by both this process and the worker
subprocess it spawns - see ``tests/integration/test_resume.py``'s module
docstring for the cross-process plumbing this mirrors: stage resolution by
reflection off ``"module:QualName"`` (``app/worker.py``'s ``_load_stage``),
and out-of-band delivery of the CAS root via an environment variable, since a
zero-arg stage has no constructor to receive one through.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.infra.cas.store import CasStore

CAS_ROOT_ENV = "YTAUTO_IT_CAS_ROOT"
"""Env var a fixture publishes the CAS root through before spawning a worker.

A fresh name rather than test_resume.py's ``YTAUTO_T14_CAS_ROOT`` - that one
is scoped to that module's own three-stage pipeline; this module is meant to
be reused by any integration test that just needs a working stage.
"""


def _write_blob(data: bytes, *, kind: str) -> ContentHash:
    """Write ``data`` into the CAS root ``CAS_ROOT_ENV`` names. Filesystem only.

    Mirrors ``app/worker.py``'s own throwaway ``:memory:`` connection - see
    that module's docstring for why ``CasStore``'s constructor needs one at
    all despite never executing a statement against it here.
    """
    conn = sqlite3.connect(":memory:")
    try:
        cas = CasStore(root=Path(os.environ[CAS_ROOT_ENV]), conn=conn)
        return cas.stage_file(data, kind=kind)
    finally:
        conn.close()


class StderrFlooder:
    """Writes 200 KB to stderr, then produces one artifact.

    Stands in for the ffmpeg stages Phase 2 adds: ffmpeg routinely logs far
    more than the 60 KB measured to deadlock the pre-fix pump (see
    ``dispatcher._pump``), and clears it in seconds under normal use.
    """

    id = "flood"
    version = 1
    depends_on: tuple[str, ...] = ()

    def fingerprint(self, ctx: JobContext) -> str:
        return "f" * 64

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        sys.stderr.write("x" * 200_000)
        sys.stderr.flush()
        digest = _write_blob(b"done", kind="text")
        return StageResult(artifacts=(ArtifactRef(name="out", kind="text", digest=digest),))
