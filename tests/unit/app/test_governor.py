import pytest

from ytauto.app.scheduler.governor import GPU_COMPUTE_CAPACITY, GPU_ENCODE_CAPACITY, Governor
from ytauto.core.errors import ValidationError


@pytest.fixture()
def governor() -> Governor:
    return Governor()


def test_gpu_compute_capacity_is_one() -> None:
    """A hard constant. Deriving it from vram_mb invites '4096 MiB, so 2 slots',
    which is exactly the VRAM exhaustion the governor exists to prevent."""
    assert GPU_COMPUTE_CAPACITY == 1
    assert Governor().available("gpu_compute") == 1


def test_a_lease_is_granted_and_released_by_scope(governor: Governor) -> None:
    with governor.lease("gpu_compute", "w1") as granted:
        assert granted is True
        assert governor.available("gpu_compute") == 0
    assert governor.available("gpu_compute") == 1


def test_a_second_simultaneous_gpu_lease_is_refused(governor: Governor) -> None:
    with governor.lease("gpu_compute", "w1") as first:
        assert first is True
        with governor.lease("gpu_compute", "w2") as second:
            assert second is False
    assert governor.available("gpu_compute") == 1


def test_a_refused_lease_does_not_consume_capacity(governor: Governor) -> None:
    """The refused caller must not decrement anything on the way out, or
    capacity leaks one slot per refusal."""
    with governor.lease("gpu_compute", "w1"):  # noqa: SIM117 - nesting mirrors "second while first is held"
        with governor.lease("gpu_compute", "w2") as second:
            assert second is False
    assert governor.available("gpu_compute") == 1


def test_a_lease_is_released_even_when_the_body_raises(governor: Governor) -> None:
    with pytest.raises(ValueError):  # noqa: SIM117 - keep raises and lease scoping visually distinct
        with governor.lease("gpu_compute", "w1"):
            raise ValueError("stage exploded")
    assert governor.available("gpu_compute") == 1


def test_release_all_frees_a_dead_worker_s_leases(governor: Governor) -> None:
    """The reaper's hook: a worker died holding a lease and cannot release it."""
    governor.lease("gpu_compute", "w1").__enter__()
    assert governor.available("gpu_compute") == 0
    assert governor.release_all("w1") == 1
    assert governor.available("gpu_compute") == 1


def test_an_unknown_pool_is_rejected(governor: Governor) -> None:
    with pytest.raises(ValidationError, match="unknown pool"):  # noqa: SIM117
        with governor.lease("nonexistent", "w1"):
            pass


def test_gpu_encode_capacity_is_one(governor: Governor) -> None:
    """The compose stages' pool, separate from gpu_compute (Task 11): a
    render taking the encode engine must not be serialised behind, or
    block, a future transcription job taking the compute engine."""
    assert GPU_ENCODE_CAPACITY == 1
    assert governor.available("gpu_encode") == 1


def test_gpu_compute_and_gpu_encode_are_independent_pools(governor: Governor) -> None:
    """Exhausting one pool must not touch the other - they model physically
    different hardware engines on the same card."""
    with governor.lease("gpu_compute", "w1") as compute_granted:
        assert compute_granted is True
        with governor.lease("gpu_encode", "w2") as encode_granted:
            assert encode_granted is True, "gpu_encode must not be blocked by gpu_compute"
        assert governor.available("gpu_encode") == 1
    assert governor.available("gpu_compute") == 1
