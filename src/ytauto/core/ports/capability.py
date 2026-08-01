"""Declarative capability metadata every provider ships.

This is what makes "keep operating costs extremely low" a system property
rather than an intention: a cost policy can prefer free and offline providers
and escalate only on explicit opt-in, because every provider states its terms
in the same shape.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from ytauto.core.errors import ValidationError


class CostModel(StrEnum):
    FREE = "free"
    PER_TOKEN = "per_token"
    PER_CHAR = "per_char"
    PER_SECOND = "per_second"
    PER_IMAGE = "per_image"


class LatencyClass(StrEnum):
    INSTANT = "instant"
    FAST = "fast"
    SLOW = "slow"


@dataclass(frozen=True)
class CapabilityDescriptor:
    """What a provider costs, needs, and is good for.

    Raises:
        ValidationError: if ``provider_id`` is empty, ``quality_tier`` is
            outside 1-5, or ``requires_gpu`` and ``vram_mb`` disagree.
    """

    provider_id: str
    version: str
    cost_model: CostModel
    latency_class: LatencyClass
    offline: bool
    requires_gpu: bool
    vram_mb: int | None
    quality_tier: int
    languages: frozenset[str]

    def __post_init__(self) -> None:
        if not self.provider_id:
            raise ValidationError("provider_id must not be empty")
        if not 1 <= self.quality_tier <= 5:
            raise ValidationError(f"quality_tier must be 1-5, got {self.quality_tier}")
        if self.requires_gpu and self.vram_mb is None:
            raise ValidationError(
                f"{self.provider_id} requires a GPU but declares no vram_mb; "
                "the resource governor cannot schedule it safely"
            )
        if not self.requires_gpu and self.vram_mb is not None:
            raise ValidationError(f"{self.provider_id} declares vram_mb but not requires_gpu")

    @property
    def is_free(self) -> bool:
        """True when using this provider costs nothing per call."""
        return self.cost_model is CostModel.FREE
