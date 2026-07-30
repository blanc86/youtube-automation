import hashlib
from pathlib import Path

import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.models.content_hash import (
    hash_bytes,
    hash_file,
    validate_digest,
)


def test_hash_bytes_matches_sha256() -> None:
    assert hash_bytes(b"hello") == hashlib.sha256(b"hello").hexdigest()


def test_hash_is_full_length_lowercase_hex() -> None:
    digest = hash_bytes(b"anything")
    assert len(digest) == 64
    assert digest == digest.lower()


def test_hash_file_matches_hash_bytes(tmp_path: Path) -> None:
    f = tmp_path / "x.bin"
    f.write_bytes(b"payload")
    assert hash_file(f) == hash_bytes(b"payload")


def test_hash_file_streams_content_larger_than_one_chunk(tmp_path: Path) -> None:
    """Chunked reading must produce the same digest as hashing in one shot."""
    payload = b"z" * (1024 * 1024 + 7)
    f = tmp_path / "big.bin"
    f.write_bytes(payload)
    assert hash_file(f) == hashlib.sha256(payload).hexdigest()


def test_validate_digest_accepts_a_real_digest() -> None:
    digest = hash_bytes(b"real")
    assert validate_digest(digest) == digest


@pytest.mark.parametrize(
    "bad",
    [
        pytest.param("", id="empty"),
        pytest.param("abc123", id="too-short"),
        pytest.param("a" * 63, id="one-short"),
        pytest.param("a" * 65, id="one-long"),
        pytest.param("A" * 64, id="uppercase"),
        pytest.param("g" * 64, id="non-hex"),
    ],
)
def test_validate_digest_rejects_malformed_input(bad: str) -> None:
    """Uppercase is rejected, not normalised: accepting both spellings would
    let one object be addressed by two different strings.
    """
    with pytest.raises(ValidationError):
        validate_digest(bad)
