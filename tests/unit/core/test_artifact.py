import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import hash_bytes


def test_holds_name_kind_and_digest() -> None:
    digest = hash_bytes(b"narration")
    ref = ArtifactRef(name="narration", kind="audio", digest=digest)
    assert (ref.name, ref.kind, ref.digest) == ("narration", "audio", digest)


def test_is_frozen() -> None:
    ref = ArtifactRef(name="n", kind="audio", digest=hash_bytes(b"x"))
    with pytest.raises(AttributeError):
        ref.name = "other"  # type: ignore[misc]


def test_rejects_a_malformed_digest() -> None:
    with pytest.raises(ValidationError):
        ArtifactRef(name="n", kind="audio", digest="not-a-hash")  # type: ignore[arg-type]


def test_rejects_an_empty_name() -> None:
    with pytest.raises(ValidationError, match="name"):
        ArtifactRef(name="", kind="audio", digest=hash_bytes(b"x"))


def test_rejects_an_empty_kind() -> None:
    with pytest.raises(ValidationError, match="kind"):
        ArtifactRef(name="n", kind="", digest=hash_bytes(b"x"))


def test_equal_refs_are_interchangeable() -> None:
    digest = hash_bytes(b"same")
    assert ArtifactRef("n", "audio", digest) == ArtifactRef("n", "audio", digest)
    assert len({ArtifactRef("n", "audio", digest), ArtifactRef("n", "audio", digest)}) == 1
