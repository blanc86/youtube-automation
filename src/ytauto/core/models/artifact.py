"""The unit of output a pipeline stage produces."""

from __future__ import annotations

from dataclasses import dataclass

from ytauto.core.errors import ValidationError
from ytauto.core.models.content_hash import ContentHash, validate_digest


@dataclass(frozen=True)
class ArtifactRef:
    """A named, content-addressed output of a stage.

    Holds a digest rather than bytes: artifacts can be gigabytes of video, and
    the pipeline passes references between stages, never payloads.

    Raises:
        ValidationError: if ``name`` or ``kind`` is empty, or ``digest`` is not
            a valid sha256 hex digest.
    """

    name: str
    kind: str
    digest: ContentHash

    def __post_init__(self) -> None:
        if not self.name:
            raise ValidationError("artifact name must not be empty")
        if not self.kind:
            raise ValidationError("artifact kind must not be empty")
        validate_digest(self.digest)
