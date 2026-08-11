import json
import os
import subprocess
import sys
from enum import StrEnum
from pathlib import Path

import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.models.content_hash import ContentHash
from ytauto.core.pipeline.fingerprint import (
    FINGERPRINT_SCHEMA_VERSION,
    FingerprintSpec,
    canonical_json,
    compute_fingerprint,
)

DIGEST_A = ContentHash("a" * 64)
DIGEST_B = ContentHash("b" * 64)


def _spec(**overrides: object) -> FingerprintSpec:
    base: dict[str, object] = {
        "stage_id": "rewrite",
        "stage_version": 1,
        "provider_id": "gemini-flash",
        "provider_version": "2026-05",
        "input_digests": (("input", DIGEST_A),),
        "settings": {"tone": "dramatic", "max_words": 900},
    }
    base.update(overrides)
    return FingerprintSpec(**base)  # type: ignore[arg-type]


def test_produces_a_full_lowercase_hex_digest() -> None:
    value = compute_fingerprint(_spec())
    assert len(value) == 64
    assert value == value.lower()
    int(value, 16)


def test_is_deterministic() -> None:
    assert compute_fingerprint(_spec()) == compute_fingerprint(_spec())


def test_dict_insertion_order_does_not_matter() -> None:
    a = _spec(settings={"tone": "dramatic", "max_words": 900})
    b = _spec(settings={"max_words": 900, "tone": "dramatic"})
    assert compute_fingerprint(a) == compute_fingerprint(b)


def test_sets_encode_in_sorted_order() -> None:
    """White-box, because a black-box pairwise comparison cannot catch this.

    Within one process, two sets built from the same elements iterate
    identically - order depends on element hashes and table size, not on
    insertion sequence - so comparing two equal sets proves nothing about
    whether a sort happened. Ten elements make an accidentally-sorted
    iteration order about a 1-in-3.6-million coincidence.
    """
    langs = {"pt", "de", "ja", "en", "fr", "ko", "it", "es", "zh", "ru"}
    encoded = json.loads(canonical_json({"langs": langs}))
    assert encoded["langs"] == sorted(langs)


def test_input_digest_order_does_matter() -> None:
    """Concatenation order changes the output, so it must change the key."""
    a = _spec(input_digests=(("p", DIGEST_A), ("q", DIGEST_B)))
    b = _spec(input_digests=(("q", DIGEST_B), ("p", DIGEST_A)))
    assert compute_fingerprint(a) != compute_fingerprint(b)


def test_swapping_two_artifact_names_changes_the_fingerprint() -> None:
    """A false HIT is the most damaging bug this module can have: the pipeline
    skips a stage and serves a different arrangement of inputs as its output."""
    a, b = ContentHash("a" * 64), ContentHash("b" * 64)
    one = _spec(input_digests=(("narration", a), ("timings", b)))
    two = _spec(input_digests=(("narration", b), ("timings", a)))
    assert compute_fingerprint(one) != compute_fingerprint(two)


def test_swapping_names_with_the_same_digest_sequence_changes_the_fingerprint() -> None:
    """Isolates the false HIT the module docstring describes, which the test
    above does not: here the *bare digest sequence* - position 0 carries digest
    ``a``, position 1 carries digest ``b`` - is identical between ``one`` and
    ``two``. Only which name is attached to which position differs. Without
    names in the payload these two are indistinguishable (see the guard-pinning
    proof in the task report, which reverts the payload construction to strip
    names and shows this exact pair of specs collide)."""
    a, b = ContentHash("a" * 64), ContentHash("b" * 64)
    one = _spec(input_digests=(("narration", a), ("timings", b)))
    two = _spec(input_digests=(("timings", a), ("narration", b)))
    assert [d for _n, d in one.input_digests] == [d for _n, d in two.input_digests], (
        "test setup bug: this test only proves anything if the bare-digest "
        "sequences are identical between one and two"
    )
    assert compute_fingerprint(one) != compute_fingerprint(two)


def test_the_same_names_and_digests_fingerprint_identically() -> None:
    a, b = ContentHash("a" * 64), ContentHash("b" * 64)
    assert compute_fingerprint(_spec(input_digests=(("n", a), ("t", b)))) == compute_fingerprint(
        _spec(input_digests=(("n", a), ("t", b)))
    )


def test_input_order_still_changes_the_fingerprint() -> None:
    """Order stays load-bearing - concatenating two clips the other way round
    produces a different video."""
    a, b = ContentHash("a" * 64), ContentHash("b" * 64)
    assert compute_fingerprint(_spec(input_digests=(("x", a), ("y", b)))) != compute_fingerprint(
        _spec(input_digests=(("y", b), ("x", a)))
    )


def test_schema_version_is_two_and_typed() -> None:
    assert FINGERPRINT_SCHEMA_VERSION == 2
    assert isinstance(FINGERPRINT_SCHEMA_VERSION, int)


@pytest.mark.parametrize(
    "field",
    ["stage_id", "provider_id", "provider_version"],
)
def test_changing_any_identity_field_changes_the_hash(field: str) -> None:
    assert compute_fingerprint(_spec()) != compute_fingerprint(_spec(**{field: "other"}))


def test_bumping_stage_version_changes_the_hash() -> None:
    assert compute_fingerprint(_spec()) != compute_fingerprint(_spec(stage_version=2))


def test_changing_a_setting_changes_the_hash() -> None:
    assert compute_fingerprint(_spec()) != compute_fingerprint(
        _spec(settings={"tone": "calm", "max_words": 900})
    )


def test_int_and_float_are_distinct_settings() -> None:
    assert compute_fingerprint(_spec(settings={"gain": 1})) != compute_fingerprint(
        _spec(settings={"gain": 1.0})
    )


def test_changing_the_schema_version_changes_every_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A future canonicalisation change must invalidate, not collide.

    Asserted against compute_fingerprint itself: round-tripping the constant
    through canonical_json would pass for any working JSON encoder even if the
    payload never carried the version at all.
    """
    before = compute_fingerprint(_spec())
    monkeypatch.setattr(
        "ytauto.core.pipeline.fingerprint.FINGERPRINT_SCHEMA_VERSION",
        FINGERPRINT_SCHEMA_VERSION + 1,
    )
    assert compute_fingerprint(_spec()) != before


def test_enums_encode_as_their_value() -> None:
    class Tone(StrEnum):
        DRAMATIC = "dramatic"

    assert compute_fingerprint(_spec(settings={"tone": Tone.DRAMATIC})) == (
        compute_fingerprint(_spec(settings={"tone": "dramatic"}))
    )


def test_bytes_do_not_collide_with_their_hex_string() -> None:
    """A false cache HIT is worse than a miss: it serves one stage's output as
    another's. Bare hex encoding would make these two settings identical."""
    as_bytes = compute_fingerprint(_spec(settings={"payload": b"\xab\xcd"}))
    as_text = compute_fingerprint(_spec(settings={"payload": "abcd"}))
    assert as_bytes != as_text


def test_bytes_fingerprint_deterministically() -> None:
    assert compute_fingerprint(_spec(settings={"payload": b"\x00\xff"})) == (
        compute_fingerprint(_spec(settings={"payload": b"\x00\xff"}))
    )


def test_paths_are_rejected() -> None:
    """A path is machine-specific; including one makes the cache non-portable."""
    with pytest.raises(ValidationError, match="path"):
        compute_fingerprint(_spec(settings={"workdir": Path("/tmp/x")}))


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_floats_are_rejected(bad: float) -> None:
    with pytest.raises(ValidationError):
        compute_fingerprint(_spec(settings={"gain": bad}))


def test_unsupported_types_are_rejected() -> None:
    with pytest.raises(ValidationError, match="object"):
        compute_fingerprint(_spec(settings={"handle": object()}))


def test_nested_structures_are_canonicalised() -> None:
    a = _spec(settings={"outer": {"b": [1, {"y": 2, "x": 1}], "a": 0}})
    b = _spec(settings={"outer": {"a": 0, "b": [1, {"x": 1, "y": 2}]}})
    assert compute_fingerprint(a) == compute_fingerprint(b)


def test_list_order_does_matter() -> None:
    a = _spec(settings={"beats": ["hook", "body"]})
    b = _spec(settings={"beats": ["body", "hook"]})
    assert compute_fingerprint(a) != compute_fingerprint(b)


def test_is_stable_across_interpreter_restarts() -> None:
    """The test that matters most: no PYTHONHASHSEED dependence, no id(), no
    insertion-order reliance. A fingerprint that varies per process silently
    disables every cache benefit and fails nothing loudly."""
    script = (
        "from ytauto.core.pipeline.fingerprint import FingerprintSpec, compute_fingerprint\n"
        "from ytauto.core.models.content_hash import ContentHash\n"
        "print(compute_fingerprint(FingerprintSpec(\n"
        "    stage_id='rewrite', stage_version=1, provider_id='gemini-flash',\n"
        "    provider_version='2026-05', input_digests=(('input', ContentHash('a'*64)),),\n"
        "    settings={'tone': 'dramatic', 'max_words': 900, 'langs': {'en','fr','de'}},\n"
        ")))\n"
    )
    seen = set()
    for seed in ("0", "1", "12345"):
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            # Inherit the environment and override only the seed. A minimal env
            # is not an option on Windows: without SystemRoot the interpreter
            # fails to initialize, and the test would fail for a reason that has
            # nothing to do with fingerprint stability.
            env={**os.environ, "PYTHONHASHSEED": seed},
            check=True,
        )
        seen.add(result.stdout.strip())

    expected = compute_fingerprint(
        _spec(settings={"tone": "dramatic", "max_words": 900, "langs": {"en", "fr", "de"}})
    )
    assert seen == {expected}, f"fingerprint varied across processes: {seen}"
