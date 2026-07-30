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
    try:
        get_logger("ytauto.test").info("written to disk", extra={"stage": "doctor"})
    finally:
        # Close only the handlers this test installed. logging.shutdown() would
        # close every handler in the interpreter, leaving closed-but-attached
        # handlers on the "ytauto" logger for the rest of the session.
        root = logging.getLogger("ytauto")
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()

    log_files = list(paths.logs.glob("*.jsonl"))
    assert log_files, "expected a .jsonl log file"
    lines = [json.loads(line) for line in log_files[0].read_text("utf-8").splitlines() if line]
    assert any(entry["msg"] == "written to disk" and entry["stage"] == "doctor" for entry in lines)


def test_configure_logging_closes_previous_handlers_on_reconfigure(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path)
    paths.ensure()

    configure_logging(paths, level="DEBUG")
    # Only FileHandler (RotatingFileHandler) closes its underlying stream and
    # sets it to None; the console StreamHandler wraps sys.stderr and is never
    # closed, so it is not part of this assertion.
    first_file_handlers = [
        handler
        for handler in logging.getLogger("ytauto").handlers
        if isinstance(handler, logging.FileHandler)
    ]
    assert first_file_handlers, "expected configure_logging to install a FileHandler"

    configure_logging(paths, level="DEBUG")
    try:
        for handler in first_file_handlers:
            assert handler.stream is None, (
                f"{handler!r} left its file descriptor open after reconfigure"
            )
    finally:
        root = logging.getLogger("ytauto")
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
