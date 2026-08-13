import json

import pytest

from ytauto.app.scheduler.worker_protocol import (
    ArtifactLine,
    Error,
    LogLine,
    Progress,
    Result,
    decode,
    encode,
)
from ytauto.core.errors import ErrorKind, ValidationError


def test_a_result_round_trips() -> None:
    msg = Result(
        job_id="j1",
        stage_id="tts",
        correlation_id="c1",
        artifacts=(ArtifactLine(name="narration", kind="blob", digest="a" * 64),),
        meta={"duration_s": 12.5},
    )
    assert decode(encode(msg)) == msg


def test_every_message_carries_its_correlation_id() -> None:
    """Read from a ContextVar instead and a relayed line gets the PARENT's id,
    destroying the per-job trail - carry-forward 1.3."""
    line = encode(
        Progress(
            job_id="j1", stage_id="tts", correlation_id="c1", fraction=0.5, note="synthesising"
        )
    )
    assert json.loads(line)["correlation_id"] == "c1"


def test_encode_emits_exactly_one_line() -> None:
    """The transport is line-delimited; an embedded newline would split one
    message into two unparseable halves."""
    line = encode(
        LogLine(
            job_id="j1",
            stage_id="tts",
            correlation_id="c1",
            level="ERROR",
            message="boom\nsecond line",
            exc=None,
        )
    )
    assert line.count("\n") == 0
    assert decode(line).message == "boom\nsecond line"


def test_an_unknown_message_type_decodes_to_none() -> None:
    """A newer worker must not be able to wedge an older parent."""
    assert (
        decode(
            json.dumps(
                {
                    "v": 1,
                    "type": "telemetry",
                    "job_id": "j1",
                    "stage_id": "s",
                    "correlation_id": "c",
                }
            )
        )
        is None
    )


def test_an_unknown_protocol_version_decodes_to_none() -> None:
    assert (
        decode(
            json.dumps(
                {
                    "v": 99,
                    "type": "progress",
                    "job_id": "j1",
                    "stage_id": "s",
                    "correlation_id": "c",
                    "fraction": 0.1,
                    "note": "",
                }
            )
        )
        is None
    )


def test_a_non_json_line_is_fatal() -> None:
    """A corrupt pipe is not survivable and must not be silently skipped."""
    with pytest.raises(ValidationError, match="not valid JSON"):
        decode("this is not json")


def test_an_error_carries_its_kind_and_retry_hint() -> None:
    msg = Error(
        job_id="j1",
        stage_id="tts",
        correlation_id="c1",
        message="429 from provider",
        kind=ErrorKind.RATE_LIMITED,
        retry_after_s=30.0,
    )
    assert decode(encode(msg)) == msg
    assert decode(encode(msg)).kind is ErrorKind.RATE_LIMITED


def test_a_known_type_missing_a_required_field_is_fatal() -> None:
    """Same protocol version, malformed payload: a same-version bug or
    corruption, not skew. Swallowing it would let a buggy worker of the
    identical version silently wedge the parent - the dispatcher expects
    exactly one terminal message per stage, and a dropped `result` leaves
    that stage waiting forever."""
    with pytest.raises(ValidationError, match="progress"):
        decode(
            json.dumps(
                {"v": 1, "type": "progress", "job_id": "j1", "stage_id": "s", "correlation_id": "c"}
            )
        )


def test_a_known_type_with_a_malformed_kind_is_fatal() -> None:
    with pytest.raises(ValidationError, match="error"):
        decode(
            json.dumps(
                {
                    "v": 1,
                    "type": "error",
                    "job_id": "j1",
                    "stage_id": "s",
                    "correlation_id": "c",
                    "message": "boom",
                    "kind": "not_a_real_kind",
                }
            )
        )
