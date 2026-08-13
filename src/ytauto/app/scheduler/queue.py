"""The persistent job queue: claim-with-lease over the ``jobs`` table.

This is what lets the application survive being killed. A worker claims a
job by taking a time-boxed lease on it; if the worker dies before the lease
expires, ``reap_expired`` returns the job to the queue so another worker can
pick it up and resume from its last completed stage.

``claim`` is the one method in this module that reads to find a candidate row
and then writes to take it - a read-then-write. It must run inside
``transaction(conn, immediate=True)``. A *deferred* transaction that reads
first (taking an implicit snapshot) and only later tries to write can be
handed ``SQLITE_BUSY_SNAPSHOT`` the moment another connection has committed a
write in between - returned immediately, without the busy handler ever
running, so ``busy_timeout`` does not help. That failure is silent in a
single-connection test and only shows up as a flaky, load-dependent bug once
two workers are genuinely racing for the same job. ``immediate=True`` avoids
it by taking the write lock at ``BEGIN``, before the ``SELECT`` runs, so a
racing claimer simply waits (or fails fast on lock contention, which callers
must already expect) instead of losing a snapshot race.

``available_at`` is what makes rate-limited retry real: it lets ``requeue``
defer a job into the future without removing it from the table, and lets
``claim`` skip anything not yet due. Its default is ``''``, which sorts
before every ISO-8601 timestamp, so a freshly enqueued job with no explicit
defer is immediately claimable.

``renew`` is scoped to ``lease_owner`` on purpose. A lease that has already
expired may have been reaped and re-claimed by a different worker by the time
the original one calls ``renew`` again; without the owner check, the original
worker could claw the job back out from under whoever now legitimately holds
it.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

from ytauto.core.models.job import JobState
from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import transaction


@dataclass(frozen=True)
class ClaimedJob:
    """What a successful ``claim`` hands back: enough to run the job."""

    job_id: str
    project_id: str
    pipeline_id: str
    attempts: int


def _offset_iso(base: str, seconds: float) -> str:
    """``base`` (an ISO-8601 UTC timestamp) shifted by ``seconds``.

    Negative values move it into the past, which is how tests construct an
    already-expired lease or an already-due ``available_at`` without sleeping.
    """
    return (datetime.fromisoformat(base) + timedelta(seconds=seconds)).isoformat()


class JobQueue:
    """Claim-with-lease over the ``jobs`` table of a migrated connection."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def enqueue(self, job_id: str, project_id: str, pipeline_id: str, *, priority: int = 0) -> None:
        """Insert a new job in the queued state, immediately claimable.

        ``available_at`` is left at its schema default (``''``), which sorts
        before every real timestamp - the job is claimable as soon as it is
        visible.

        Raises:
            sqlite3.IntegrityError: ``job_id`` is already present - ``jobs.id``
                is the primary key.
            sqlite3.Error: the insert fails for another reason.
        """
        now = utc_now_iso()
        with transaction(self._conn):
            self._conn.execute(
                """
                INSERT INTO jobs
                    (id, project_id, pipeline_id, state, priority, attempts,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 0, ?, ?)
                """,
                (job_id, project_id, pipeline_id, JobState.QUEUED.value, priority, now, now),
            )

    def claim(self, owner: str, *, lease_s: float) -> ClaimedJob | None:
        """Atomically claim the highest-priority claimable job, if any.

        "Claimable" means ``state = 'queued'`` and ``available_at`` is not in
        the future. Ties break on ``created_at`` (oldest first). Runs as one
        ``transaction(conn, immediate=True)`` - see the module docstring for
        why a deferred transaction here would be a load-dependent bug.

        Raises:
            sqlite3.OperationalError: the write lock could not be acquired
                within ``busy_timeout`` - legitimate contention from another
                claimer or the reaper.
        """
        now = utc_now_iso()
        claimed: ClaimedJob | None = None
        with transaction(self._conn, immediate=True):
            row = self._conn.execute(
                """
                SELECT id, project_id, pipeline_id, attempts
                FROM jobs
                WHERE state = ? AND available_at <= ?
                ORDER BY priority DESC, created_at
                LIMIT 1
                """,
                (JobState.QUEUED.value, now),
            ).fetchone()
            if row is not None:
                attempts = int(row["attempts"]) + 1
                lease_expires_at = _offset_iso(now, lease_s)
                self._conn.execute(
                    """
                    UPDATE jobs
                    SET state = ?, lease_owner = ?, lease_expires_at = ?,
                        attempts = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (JobState.RUNNING.value, owner, lease_expires_at, attempts, now, row["id"]),
                )
                claimed = ClaimedJob(
                    job_id=row["id"],
                    project_id=row["project_id"],
                    pipeline_id=row["pipeline_id"],
                    attempts=attempts,
                )
        return claimed

    def renew(self, job_id: str, owner: str, *, lease_s: float) -> bool:
        """Extend the lease on a job this worker still owns. Returns whether it did.

        Scoped to ``lease_owner`` - see the module docstring for why a worker
        whose lease already expired and was reaped must not be able to renew
        it back out from under whoever holds it now.

        Raises:
            sqlite3.Error: the update fails.
        """
        now = utc_now_iso()
        lease_expires_at = _offset_iso(now, lease_s)
        with transaction(self._conn):
            cursor = self._conn.execute(
                """
                UPDATE jobs
                SET lease_expires_at = ?, updated_at = ?
                WHERE id = ? AND lease_owner = ?
                """,
                (lease_expires_at, now, job_id, owner),
            )
            renewed = cursor.rowcount == 1
        return renewed

    def requeue(
        self, job_id: str, *, available_in_s: float = 0.0, error: str | None = None
    ) -> None:
        """Return a job to the queue, deferred until ``available_in_s`` from now.

        Clears any lease, so a job requeued from ``running`` is immediately
        eligible for a new claim once ``available_at`` passes. ``error``, when
        given, replaces ``last_error``; when omitted it is cleared, since a
        fresh requeue with no error means the previous one no longer applies.

        Raises:
            sqlite3.Error: the update fails.
        """
        now = utc_now_iso()
        available_at = _offset_iso(now, available_in_s)
        with transaction(self._conn):
            self._conn.execute(
                """
                UPDATE jobs
                SET state = ?, lease_owner = NULL, lease_expires_at = NULL,
                    available_at = ?, updated_at = ?, last_error = ?
                WHERE id = ?
                """,
                (JobState.QUEUED.value, available_at, now, error, job_id),
            )

    def complete(self, job_id: str) -> None:
        """Mark a job as succeeded. Terminal - never claimable again.

        Raises:
            sqlite3.Error: the update fails.
        """
        now = utc_now_iso()
        with transaction(self._conn):
            self._conn.execute(
                """
                UPDATE jobs
                SET state = ?, lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
                WHERE id = ?
                """,
                (JobState.SUCCEEDED.value, now, job_id),
            )

    def fail(self, job_id: str, error: str) -> None:
        """Mark a job as terminally failed, recording ``error``. Never claimable again.

        Raises:
            sqlite3.Error: the update fails.
        """
        now = utc_now_iso()
        with transaction(self._conn):
            self._conn.execute(
                """
                UPDATE jobs
                SET state = ?, lease_owner = NULL, lease_expires_at = NULL,
                    updated_at = ?, last_error = ?
                WHERE id = ?
                """,
                (JobState.FAILED.value, now, error, job_id),
            )

    def reap_expired(self, *, now: str | None = None) -> tuple[str, ...]:
        """Return every job whose lease has expired back to the queue.

        Returns the ids of the jobs reaped, in no particular order beyond
        whatever the single ``UPDATE ... RETURNING`` produces.

        A single ``UPDATE ... RETURNING`` does the select-and-take in one
        statement: the ``WHERE`` clause re-checked at write time *is* the
        read, so there is no separate read-then-write window here for
        ``immediate=True`` to protect - unlike ``claim``, which must look at a
        row and decide before it writes.

        Raises:
            sqlite3.Error: the update fails.
        """
        ts = now if now is not None else utc_now_iso()
        with transaction(self._conn):
            rows = self._conn.execute(
                """
                UPDATE jobs
                SET state = ?, lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
                WHERE state = ? AND lease_expires_at < ?
                RETURNING id
                """,
                (JobState.QUEUED.value, ts, JobState.RUNNING.value, ts),
            ).fetchall()
        return tuple(row["id"] for row in rows)
