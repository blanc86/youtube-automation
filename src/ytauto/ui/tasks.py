"""Running the two slow operations off the request thread, and reporting on them.

Two things this UI does take far longer than a browser will wait: a render
(10-120 seconds, mostly ffmpeg) and a B-roll add (two full transcodes of the
source clip, with a 600-second ceiling). Both are started by a POST that must
come back immediately, and both are then watched by the page polling
``/api/tasks/<id>``.

**A thread, not a process, and one connection each.** The work these tasks do
is already mostly waiting on subprocesses the dispatcher spawns, so a thread
is the right weight. What matters is the database: ``infra.db.engine``'s own
module docstring is explicit that a connection carrying transactions must not
be shared across threads - savepoint bookkeeping is per-connection and two
threads interleaving on one connection can destroy each other's frames. So
``TaskManager`` hands the worker a *factory*, not a connection, and the
function it runs opens and closes its own. Nothing crosses the boundary
except the ``TaskRecord``.

**Nothing here outlives the manager.** ``close()`` joins every thread it
started. That matters for more than tidiness: the test suite promotes
``ResourceWarning`` and ``PytestUnraisableExceptionWarning`` to errors, so a
worker thread still holding a SQLite connection when a test ends is a failed
test, not a warning nobody reads.
"""

from __future__ import annotations

import threading
import traceback
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Literal

TaskState = Literal["running", "succeeded", "failed"]


@dataclass(frozen=True)
class TaskRecord:
    """A snapshot of one background task.

    Frozen and copied out under the lock, so a caller rendering a status page
    can never observe a half-updated record or watch fields change underneath
    it mid-template.

    ``detail`` is the human-readable outcome - the export directory on a
    successful render, the new clip id on a successful add, the error text on
    a failure. ``payload`` carries anything the page wants to act on rather
    than print (currently the export directory, which the project page shows
    as its own line).
    """

    id: str
    kind: str
    label: str
    state: TaskState = "running"
    detail: str = ""
    payload: dict[str, str] = field(default_factory=dict)

    def as_json(self) -> dict[str, object]:
        return {
            "id": self.id,
            "kind": self.kind,
            "label": self.label,
            "state": self.state,
            "detail": self.detail,
            "payload": dict(self.payload),
            "done": self.state != "running",
        }


class TaskBusy(Exception):
    """A task with this key is already running.

    Raised rather than silently queueing a second one: two concurrent renders
    of the same project would both drive the same dispatcher against the same
    job rows, and two concurrent B-roll adds of the same file would race the
    duplicate-source check ``BrollLibrary.add`` makes before transcoding.
    Refusing is both correct and what the user meant - they clicked twice.
    """


class TaskManager:
    """Starts background tasks, keeps their last known state, and joins them.

    Keyed work: ``submit`` takes a ``key`` (``"render:<project id>"``,
    ``"broll"``) and refuses a second task under a key that is still running.
    Finished records are kept - the page that started a task needs to be able
    to read its outcome after it ends, and a personal tool run for an
    afternoon will accumulate a handful of them, not thousands.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._records: dict[str, TaskRecord] = {}
        self._by_key: dict[str, str] = {}
        self._threads: list[threading.Thread] = []

    def submit(
        self,
        *,
        key: str,
        kind: str,
        label: str,
        work: Callable[[], tuple[str, dict[str, str]]],
    ) -> TaskRecord:
        """Start ``work`` on a new thread and return its initial record.

        ``work`` returns ``(detail, payload)`` on success and raises on
        failure; anything it raises is caught, recorded as the task's
        ``detail``, and the task is marked ``failed``. A background thread
        that let an exception escape would print a traceback to a console
        nobody is looking at and leave the page polling a task that never
        finishes.

        Raises:
            TaskBusy: a task under ``key`` is still running.
        """
        with self._lock:
            running = self._by_key.get(key)
            if running is not None and self._records[running].state == "running":
                raise TaskBusy(key)
            record = TaskRecord(id=uuid.uuid4().hex, kind=kind, label=label)
            self._records[record.id] = record
            self._by_key[key] = record.id
            thread = threading.Thread(
                target=self._run, args=(record.id, work), name=f"ytauto-{kind}", daemon=True
            )
            self._threads.append(thread)
        thread.start()
        return record

    def _run(self, task_id: str, work: Callable[[], tuple[str, dict[str, str]]]) -> None:
        try:
            detail, payload = work()
        except Exception as exc:
            # Deliberately broad - see submit()'s docstring. A background
            # thread that let anything escape would print a traceback to a
            # console nobody is watching and leave the page polling forever.
            self._finish(
                task_id,
                state="failed",
                detail=_describe(exc),
                payload={"traceback": traceback.format_exc()},
            )
            return
        self._finish(task_id, state="succeeded", detail=detail, payload=payload)

    def _finish(
        self, task_id: str, *, state: TaskState, detail: str, payload: dict[str, str]
    ) -> None:
        with self._lock:
            self._records[task_id] = replace(
                self._records[task_id], state=state, detail=detail, payload=payload
            )

    def get(self, task_id: str) -> TaskRecord | None:
        with self._lock:
            return self._records.get(task_id)

    def for_key(self, key: str) -> TaskRecord | None:
        """The most recent task submitted under ``key``, running or not."""
        with self._lock:
            task_id = self._by_key.get(key)
            return self._records.get(task_id) if task_id is not None else None

    def close(self, timeout: float = 60.0) -> None:
        """Join every thread this manager started.

        ``timeout`` is per thread and generous: a render that is genuinely
        mid-ffmpeg cannot be interrupted safely, and the alternative to
        waiting is leaving a SQLite connection open in a thread nobody
        owns.
        """
        with self._lock:
            threads = list(self._threads)
            self._threads.clear()
        for thread in threads:
            thread.join(timeout)


def _describe(exc: BaseException) -> str:
    """A one-line description of a failure, for a banner in a browser.

    Includes the exception class when the message alone would be
    uninformative - a bare ``PermissionError`` stringifies to something like
    ``[Errno 13] Permission denied: '...'`` which reads fine, but several of
    this project's own error types carry a message and nothing else, and a
    ``TimeoutExpired`` stringifies to a sentence about a command line.
    """
    message = str(exc).strip()
    return message or exc.__class__.__name__
