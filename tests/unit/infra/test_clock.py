from datetime import datetime

from ytauto.infra.clock import utc_now_iso


def test_returns_parseable_iso8601() -> None:
    parsed = datetime.fromisoformat(utc_now_iso())
    assert parsed.tzinfo is not None


def test_offset_is_explicitly_utc() -> None:
    value = utc_now_iso()
    assert value.endswith("+00:00"), value


def test_values_sort_chronologically_as_plain_strings() -> None:
    """CAS eviction orders by this column as TEXT, so string order must be time order."""
    first = utc_now_iso()
    second = utc_now_iso()
    assert first <= second
