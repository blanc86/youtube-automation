import json
import logging
from pathlib import Path

from ytauto.infra.logging import (
    JsonFormatter,
    bind_correlation_id,
    configure_logging,
    current_correlation_id,
    get_logger,
)
from ytauto.infra.paths import AppPaths


def _format_one(record: logging.LogRecord) -> dict[str, object]:
    return json.loads(JsonFormatter().format(record))


def _make_record(**extra: object) -> logging.LogRecord:
    record = logging.LogRecord(
        name="ytauto.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello %s",
        args=("world",),
        exc_info=None,
    )
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_formatter_emits_valid_json_with_required_fields() -> None:
    payload = _format_one(_make_record())
    assert payload["level"] == "INFO"
    assert payload["logger"] == "ytauto.test"
    assert payload["msg"] == "hello world"
    assert "ts" in payload


def test_formatter_includes_extra_fields() -> None:
    payload = _format_one(_make_record(stage="rewrite", project_id="abc"))
    assert payload["stage"] == "rewrite"
    assert payload["project_id"] == "abc"


def test_formatter_excludes_internal_logrecord_attributes() -> None:
    payload = _format_one(_make_record())
    for noisy in ("args", "msecs", "relativeCreated", "pathname", "exc_text"):
        assert noisy not in payload


def test_bind_correlation_id_generates_one_when_absent() -> None:
    cid = bind_correlation_id()
    assert len(cid) == 32
    assert current_correlation_id() == cid


def test_bind_correlation_id_accepts_explicit_value() -> None:
    bind_correlation_id("job-42")
    assert current_correlation_id() == "job-42"


def test_correlation_id_appears_in_formatted_output() -> None:
    bind_correlation_id("job-99")
    payload = _format_one(_make_record())
    assert payload["correlation_id"] == "job-99"


def test_configure_logging_writes_a_log_file(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path)
    paths.ensure()
    configure_logging(paths, level="DEBUG")
    get_logger("ytauto.test").info("written to disk", extra={"stage": "doctor"})

    logging.shutdown()
    log_files = list(paths.logs.glob("*.jsonl"))
    assert log_files, "expected a .jsonl log file"
    lines = [json.loads(line) for line in log_files[0].read_text("utf-8").splitlines() if line]
    assert any(entry["msg"] == "written to disk" and entry["stage"] == "doctor" for entry in lines)
