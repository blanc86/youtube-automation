"""Shared fixtures for the integration suite.

``dispatcher_env`` builds a real ``Dispatcher`` wired to spawn genuine
``python -m ytauto.app.worker`` subprocesses, wrapping the cross-process
plumbing ``tests/integration/test_resume.py``'s module docstring explains:
stage resolution by reflection off ``"module:QualName"``, ``PYTHONPATH``
propagation so the subprocess can import a stage from ``tests/``, and
out-of-band CAS-root delivery via an environment variable since a zero-arg
stage has no constructor to receive one through.
"""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from ytauto.app.scheduler.dispatcher import Dispatcher
from ytauto.app.scheduler.governor import Governor
from ytauto.app.scheduler.queue import JobQueue
from ytauto.app.worker import _load_stage
from ytauto.core.pipeline.graph import Pipeline
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations

from .stages import CAS_ROOT_ENV

_TESTS_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class DispatcherEnv:
    """One dispatcher, one enqueued job, ready for a test to drive ``tick()``."""

    dispatcher: Dispatcher
    conn: sqlite3.Connection
    job_id: str

    def stage_status(self, stage_id: str) -> str | None:
        row = self.conn.execute(
            "SELECT status FROM job_stages WHERE job_id = ? AND stage_id = ?",
            (self.job_id, stage_id),
        ).fetchone()
        return str(row["status"]) if row is not None else None


@pytest.fixture()
def dispatcher_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Callable[..., DispatcherEnv]]:
    """Factory fixture: ``dispatcher_env(stage=..., pump_deadline_s=..., job_id=...)``.

    ``stage`` is the ``"module:QualName"`` string a worker subprocess resolves
    via reflection (``app/worker.py``'s ``_load_stage`` - the same convention
    ``dispatcher._build_assignment`` already uses), resolved here too so the
    single-stage ``Pipeline`` this builds holds the exact same kind of object
    a real dispatcher would. ``job_id`` defaults to ``"job"`` since no current
    caller needs more than one job per test.
    """
    connections: list[sqlite3.Connection] = []
    existing = os.environ.get("PYTHONPATH")
    pythonpath = os.pathsep.join([str(_TESTS_ROOT), existing]) if existing else str(_TESTS_ROOT)
    monkeypatch.setenv("PYTHONPATH", pythonpath)

    def _make(*, stage: str, pump_deadline_s: float = 1800.0, job_id: str = "job") -> DispatcherEnv:
        conn = connect(tmp_path / f"{job_id}.db")
        apply_migrations(conn)
        connections.append(conn)
        cas = CasStore(root=tmp_path / "cas", conn=conn)
        artifacts = ArtifactStore(cas, conn)
        queue = JobQueue(conn)
        monkeypatch.setenv(CAS_ROOT_ENV, str(cas.root))

        stage_obj = _load_stage(stage)
        pipeline_id = f"it-{stage_obj.id}"
        pipeline = Pipeline(id=pipeline_id, stages=(stage_obj,))
        dispatcher = Dispatcher(
            conn,
            cas,
            artifacts,
            Governor(),
            queue,
            pipelines={pipeline_id: pipeline},
            pump_deadline_s=pump_deadline_s,
        )
        queue.enqueue(job_id, "proj-1", pipeline_id)
        return DispatcherEnv(dispatcher=dispatcher, conn=conn, job_id=job_id)

    yield _make

    for conn in connections:
        conn.close()
