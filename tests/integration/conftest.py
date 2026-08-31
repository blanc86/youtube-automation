"""Shared fixtures for the integration suite.

``dispatcher_env`` builds a real ``Dispatcher`` wired to spawn genuine
``python -m ytauto.app.worker`` subprocesses, wrapping the cross-process
plumbing ``tests/integration/test_resume.py``'s module docstring explains:
stage resolution through ``ytauto.stages`` entry points (declared in
``tests/ytauto_it_stages-0.0.0.dist-info``) and ``PYTHONPATH`` propagation, so
that both this process and the subprocess can discover *and* import the same
stage factories.

Both halves of that are load-bearing. The dispatcher needs in-process stage
objects to compute fingerprints and walk the DAG; the worker needs to
construct the same stage again on the far side of a pipe. They agree because
they call the same ``app.registry`` function against the same entry-point
table, not because the assignment carries an import path.
"""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from ytauto.app.registry import build_pipeline
from ytauto.app.scheduler.dispatcher import Dispatcher
from ytauto.app.scheduler.governor import Governor
from ytauto.app.scheduler.queue import JobQueue
from ytauto.app.services.projects import ProjectService
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations

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
    """Factory fixture: ``dispatcher_env(pipeline_id=..., pump_deadline_s=..., job_id=...)``.

    ``pipeline_id`` is resolved through ``app.registry.build_pipeline``, which
    assembles every stage registered under that id - the same table the
    worker subprocess resolves its single stage from, so the two cannot drift.

    A real ``projects`` row is created and the job enqueued against it:
    ``Dispatcher.tick`` reads the project's settings into every
    ``JobContext`` now, and a job pointing at a project that does not exist is
    unrunnable rather than settingless. ``job_id`` defaults to ``"job"`` since
    no current caller needs more than one job per test.
    """
    connections: list[sqlite3.Connection] = []
    existing = os.environ.get("PYTHONPATH")
    pythonpath = os.pathsep.join([str(_TESTS_ROOT), existing]) if existing else str(_TESTS_ROOT)
    monkeypatch.setenv("PYTHONPATH", pythonpath)

    def _make(
        *,
        pipeline_id: str,
        pump_deadline_s: float = 1800.0,
        job_id: str = "job",
        settings: dict[str, object] | None = None,
    ) -> DispatcherEnv:
        conn = connect(tmp_path / f"{job_id}.db")
        apply_migrations(conn)
        connections.append(conn)
        cas = CasStore(root=tmp_path / "cas", conn=conn)
        artifacts = ArtifactStore(cas, conn)
        queue = JobQueue(conn)

        project_settings = {} if settings is None else settings
        project_id = ProjectService(conn).create(
            slug=job_id, title=job_id, story_digest=None, settings=project_settings
        )
        pipeline = build_pipeline(pipeline_id, cas, project_settings)
        dispatcher = Dispatcher(
            conn,
            cas,
            artifacts,
            Governor(),
            queue,
            pipelines={pipeline_id: pipeline},
            pump_deadline_s=pump_deadline_s,
        )
        queue.enqueue(job_id, project_id, pipeline_id)
        return DispatcherEnv(dispatcher=dispatcher, conn=conn, job_id=job_id)

    yield _make

    for conn in connections:
        conn.close()
