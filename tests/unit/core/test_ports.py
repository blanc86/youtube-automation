import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.ports.capability import CapabilityDescriptor, CostModel, LatencyClass
from ytauto.core.ports.providers import (
    ImageGenerator,
    Publisher,
    ScriptGenerator,
    SpeechSynthesizer,
    StorySource,
    ThumbnailRenderer,
    Transcriber,
    VisualStrategy,
)


def _descriptor(**overrides: object) -> CapabilityDescriptor:
    base: dict[str, object] = {
        "provider_id": "edge-tts",
        "version": "7.0",
        "cost_model": CostModel.FREE,
        "latency_class": LatencyClass.FAST,
        "offline": False,
        "requires_gpu": False,
        "vram_mb": None,
        "quality_tier": 4,
        "languages": frozenset({"en", "fr"}),
    }
    base.update(overrides)
    return CapabilityDescriptor(**base)  # type: ignore[arg-type]


def test_descriptor_is_frozen() -> None:
    with pytest.raises(AttributeError):
        _descriptor().provider_id = "other"  # type: ignore[misc]


@pytest.mark.parametrize("tier", [0, 6, -1])
def test_quality_tier_must_be_one_to_five(tier: int) -> None:
    with pytest.raises(ValidationError, match="quality_tier"):
        _descriptor(quality_tier=tier)


@pytest.mark.parametrize("tier", [1, 5])
def test_quality_tier_accepts_both_boundaries(tier: int) -> None:
    """Pins the boundaries positively as well as negatively.

    Without this, mutating the check to the off-by-one ``1 < tier < 5`` passes
    every rejection test - 0, 6 and -1 stay rejected - because nothing ever
    constructs a descriptor at 1 or 5 to notice they became wrongly rejected.
    The fixture default is 4, an interior value.
    """
    assert _descriptor(quality_tier=tier).quality_tier == tier


def test_a_gpu_provider_must_declare_its_vram() -> None:
    """The governor sizes GPU leases from this; None would mean 'unbounded'
    on a 4 GB card."""
    with pytest.raises(ValidationError, match="vram_mb"):
        _descriptor(requires_gpu=True, vram_mb=None)


def test_a_gpu_provider_with_vram_is_accepted() -> None:
    assert _descriptor(requires_gpu=True, vram_mb=2048).vram_mb == 2048


def test_a_non_gpu_provider_may_not_claim_vram() -> None:
    with pytest.raises(ValidationError, match="vram_mb"):
        _descriptor(requires_gpu=False, vram_mb=2048)


def test_free_providers_are_identified() -> None:
    assert _descriptor(cost_model=CostModel.FREE).is_free
    assert not _descriptor(cost_model=CostModel.PER_TOKEN).is_free


def test_empty_provider_id_is_rejected() -> None:
    with pytest.raises(ValidationError, match="provider_id"):
        _descriptor(provider_id="")


@pytest.mark.parametrize(
    "port",
    [
        StorySource,
        ScriptGenerator,
        SpeechSynthesizer,
        Transcriber,
        VisualStrategy,
        ImageGenerator,
        ThumbnailRenderer,
        Publisher,
    ],
)
def test_every_port_requires_a_capability_descriptor(port: type) -> None:
    """Provider selection reads `capabilities` on every port uniformly."""
    assert "capabilities" in port.__annotations__ or hasattr(port, "capabilities")


def test_a_conforming_synthesizer_satisfies_the_protocol() -> None:
    class Fake:
        capabilities = _descriptor()

        def synthesize(self, text: str, *, voice: str) -> bytes:
            return b""

    assert isinstance(Fake(), SpeechSynthesizer)


def test_a_synthesizer_missing_synthesize_does_not_satisfy_it() -> None:
    class Fake:
        capabilities = _descriptor()

    assert not isinstance(Fake(), SpeechSynthesizer)
