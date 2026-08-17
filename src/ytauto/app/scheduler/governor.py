"""The resource governor: an in-memory lease broker for finite local resources.

The dispatcher owns the single ``Governor`` instance; workers never talk to
it directly. It is in-memory and single-process by design - it is rebuilt on
every restart, which is correct, because no lease should ever survive a
crash. Persisting it would let a stale lease from a dead process outlive the
process and permanently wedge a pool.

``gpu_compute`` and ``gpu_encode`` are populated. The remaining two pools the
design spec describes (``cpu_heavy``, ``net_api``) are still added when the
work that needs them exists - there is no CPU-bound or network-rate-limited
provider to pace yet, and standing up a pool with nothing to test it against
would just be dead capacity. ``gpu_encode`` is different: Phase 2a's compose
stages (``app.stages.compose.ComposeStage``) are the first work that
genuinely contends for GPU encode hardware (NVENC/QSV/AMF) rather than GPU
compute (a future Whisper-based ``Transcriber``, still Phase 2b), and those
are physically different engines on the same card - a render taking the
render engine must not be serialised behind, or block, a transcription
taking the compute engine, which is exactly what one shared pool would do.

``GPU_COMPUTE_CAPACITY``/``GPU_ENCODE_CAPACITY`` are both the hard-coded
integer ``1``, never derived from ``infra.gpu.detect()``'s reported VRAM.
This exists specifically to prevent concurrent GPU work from exhausting 4 GB
of VRAM nondeterministically - described in the design spec as the worst
class of bug to diagnose in this system. Deriving capacity from a MiB figure
invites a "4096 MiB, so 2 slots" mistake that reintroduces exactly that
failure.

``lease`` yields ``False`` on refusal rather than raising. A refusal is a
normal scheduling outcome: the dispatcher's response to a full pool is to try
a different stage, not to catch an exception. Every caller of ``lease`` must
be written around that calling convention.

``lease`` returns a plain object implementing the context manager protocol
rather than a ``@contextmanager``-decorated generator. A generator-based
context manager that is never explicitly closed (exactly the shape of a
worker that died mid-stage) only runs its ``finally`` block when the
suspended generator is garbage-collected - and if ``release_all`` has by
then already freed the same lease, that deferred, GC-timed ``finally`` fires
a second time and either double-credits the pool or crashes trying to remove
an already-removed entry. A plain object has no such implicit cleanup: it
only ever releases when something calls ``release()`` (via ``__exit__`` or
``Governor.release_all``), and that call is idempotent, so the pool's
capacity can never be double-credited no matter what order those two paths
fire in, or whether either fires at all before the object is dropped.

``release_all(owner)`` is the reaper's hook: a worker that died holding a
lease cannot release it itself, so the dispatcher calls this for every owner
it reaps to make sure the pool does not stay permanently short a slot.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from types import TracebackType

from ytauto.core.errors import ValidationError

GPU_COMPUTE_CAPACITY: int = 1
GPU_ENCODE_CAPACITY: int = 1

_CAPACITIES: dict[str, int] = {
    "gpu_compute": GPU_COMPUTE_CAPACITY,
    "gpu_encode": GPU_ENCODE_CAPACITY,
}

KNOWN_POOLS: frozenset[str] = frozenset(_CAPACITIES)
"""Every pool name a ``Governor`` will accept. Public so ``Dispatcher._spawn``
can validate a stage's own ``gpu_pool`` at the point it reads it - naming the
offending stage in the error - rather than relying on ``Governor.lease``'s
own ``unknown pool`` ``ValidationError``, which fires just as reliably but
with no stage context attached."""


class _Lease(AbstractContextManager[bool]):
    """One outstanding request for a slot in a pool, returned by ``Governor.lease``.

    ``release()`` is idempotent, guarded by ``_active``: it is safe to call
    from ``__exit__`` (the normal path, when a stage finishes) and from
    ``Governor.release_all`` (the reaper's forced path, when a worker died
    holding it), in either order, and safe even if neither path ever runs -
    a refused lease starts with ``_active`` already ``False``, so it never
    attempts to release a slot it was never granted.
    """

    def __init__(self, governor: Governor, pool: str, owner: str, granted: bool) -> None:
        self._governor = governor
        self.pool = pool
        self.owner = owner
        self.granted = granted
        self._active = granted

    def __enter__(self) -> bool:
        return self.granted

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()

    def release(self) -> None:
        """Give the slot back, if this lease is still holding one."""
        if self._active:
            self._active = False
            self._governor._release(self)


class Governor:
    """A lease broker over a fixed set of named pools, each of fixed capacity.

    ``_available`` tracks how many slots each pool has left; ``_holders``
    tracks the live ``_Lease`` objects granted from each pool, so
    ``release_all(owner)`` can find and free every lease a given owner holds
    without knowing which pools it used.
    """

    def __init__(self) -> None:
        self._capacities: dict[str, int] = dict(_CAPACITIES)
        self._available: dict[str, int] = dict(_CAPACITIES)
        self._holders: dict[str, list[_Lease]] = {pool: [] for pool in _CAPACITIES}

    def lease(self, pool: str, owner: str) -> AbstractContextManager[bool]:
        """Request one slot in ``pool`` for ``owner``, held for the ``with`` body.

        The returned context manager yields ``True`` if a slot was granted,
        ``False`` if the pool was already at capacity. A refusal is a normal
        scheduling outcome, not an error: the caller is expected to check the
        yielded value and try something else, never to treat ``False`` as
        exceptional. A granted slot is always released when the ``with``
        block exits, whether it exits normally or via an exception.

        Raises:
            ValidationError: ``pool`` is not a known pool name.
        """
        if pool not in self._capacities:
            raise ValidationError(f"unknown pool: {pool!r}")
        granted = self._available[pool] > 0
        if granted:
            self._available[pool] -= 1
        held = _Lease(self, pool, owner, granted)
        if granted:
            self._holders[pool].append(held)
        return held

    def _release(self, held: _Lease) -> None:
        """Return ``held``'s slot to its pool and drop it from bookkeeping.

        Called at most once per ``_Lease`` - see ``_Lease.release`` for the
        idempotency guard that makes that true regardless of whether
        ``__exit__`` or ``release_all`` (or both, or neither) triggers it.
        """
        self._available[held.pool] += 1
        holders = self._holders[held.pool]
        if held in holders:
            holders.remove(held)

    def release_all(self, owner: str) -> int:
        """Free every lease ``owner`` currently holds, across every pool.

        This is the reaper's hook: a worker that died mid-stage cannot run
        its own ``__exit__`` to release its lease, so the dispatcher calls
        this for the dead owner once it reaps the job to put the slot back
        into circulation.

        Returns the number of leases freed.
        """
        freed = 0
        for holders in self._holders.values():
            for held in list(holders):
                if held.owner == owner:
                    held.release()
                    freed += 1
        return freed

    def available(self, pool: str) -> int:
        """How many free slots ``pool`` currently has.

        Raises:
            ValidationError: ``pool`` is not a known pool name.
        """
        if pool not in self._capacities:
            raise ValidationError(f"unknown pool: {pool!r}")
        return self._available[pool]
