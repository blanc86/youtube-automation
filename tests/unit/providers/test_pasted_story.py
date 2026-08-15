"""``PastedStorySource``: a verbatim, UTF-8 read of a local text file.

Nothing here exercises ``IngestStory`` - that stage's tests live in
``tests/unit/app/stages/test_ingest_story.py``, grouped with the rest of the
stage suite even though the class itself is implemented in
``ytauto.providers.story.pasted`` (see that module's docstring for why the
stage cannot live under ``app/``).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.ports.capability import CostModel
from ytauto.providers.story.pasted import PastedStorySource


def test_a_pasted_story_is_read_verbatim(tmp_path: Path) -> None:
    path = tmp_path / "story.txt"
    path.write_text("The train never stopped.\n", encoding="utf-8")
    assert PastedStorySource().fetch(str(path)) == "The train never stopped.\n"


def test_a_missing_story_file_is_a_fatal_provider_error(tmp_path: Path) -> None:
    with pytest.raises(ProviderError) as exc:
        PastedStorySource().fetch(str(tmp_path / "absent.txt"))
    assert exc.value.kind is ErrorKind.FATAL, "a missing file will not appear on retry"


def test_a_story_file_that_is_not_valid_utf8_is_a_fatal_provider_error(tmp_path: Path) -> None:
    path = tmp_path / "story.txt"
    path.write_bytes(b"\xff\xfe not valid utf-8")
    with pytest.raises(ProviderError) as exc:
        PastedStorySource().fetch(str(path))
    assert exc.value.kind is ErrorKind.FATAL, "a bad encoding will not appear on retry"


def test_the_capability_descriptor_declares_a_free_offline_no_gpu_provider() -> None:
    """Ambiguity resolution #4: this reads a local file, so no GPU, no
    network, no cost - the descriptor must say so."""
    caps = PastedStorySource.capabilities
    assert caps.provider_id == "pasted"
    assert caps.cost_model is CostModel.FREE
    assert caps.offline is True
    assert caps.requires_gpu is False
    assert caps.vram_mb is None
