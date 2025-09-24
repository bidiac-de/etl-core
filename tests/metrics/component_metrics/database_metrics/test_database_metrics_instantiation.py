from datetime import datetime, timedelta
import pytest

from etl_core.metrics.component_metrics.database_metrics.database_metrics import (
    DatabaseMetrics,
    ReadMetrics,
    WriteMetrics,
)


@pytest.fixture()
def started_and_proc():
    return datetime(2025, 1, 1, 12, 0, 0), timedelta(seconds=5)


def test_database_metrics_instantiation_and_repr(started_and_proc):
    started, proc = started_and_proc
    m = DatabaseMetrics.model_construct()
    # set through property setters for validation
    m.started_at = started
    m.processing_time = proc
    m.error_count = 1
    m.lines_received = 2
    m.lines_forwarded = 3
    # direct attributes: use object.__setattr__ to avoid pydantic field handling
    object.__setattr__(m, "query_execution_time", 1.5)

    assert m.started_at == started
    assert m.processing_time == proc
    assert m.error_count == 1
    assert m.lines_received == 2
    assert m.lines_forwarded == 3
    assert m.query_execution_time == 1.5

    r = repr(m)
    assert "query_execution_time=1.5" in r


def test_read_metrics_instantiation_and_repr():
    started = datetime(2025, 1, 1, 12, 0, 0)
    proc = timedelta(seconds=2)
    m = ReadMetrics.model_construct()
    m.started_at = started
    m.processing_time = proc
    m.error_count = 0
    m.lines_received = 4
    m.lines_forwarded = 5
    object.__setattr__(m, "query_execution_time", 0.25)
    object.__setattr__(m, "lines_read", 10)

    assert m.lines_read == 10
    assert m.query_execution_time == 0.25
    r = repr(m)
    assert "lines_read=10" in r
    assert "query_execution_time=0.25" in r


def test_write_metrics_instantiation_and_repr():
    started = datetime(2025, 1, 1, 12, 0, 0)
    proc = timedelta(seconds=3)
    m = WriteMetrics.model_construct()
    m.started_at = started
    m.processing_time = proc
    m.error_count = 2
    m.lines_received = 7
    m.lines_forwarded = 8
    object.__setattr__(m, "query_execution_time", 0.75)
    object.__setattr__(m, "lines_written", 11)

    assert m.lines_written == 11
    assert m.lines_forwarded == 8
    r = repr(m)
    assert "lines_written=11" in r
    assert "query_execution_time=0.75" in r


def test_component_metrics_validation_errors():
    started = datetime(2025, 1, 1, 12, 0, 0)
    proc = timedelta(seconds=1)
    m = DatabaseMetrics.model_construct()
    m.started_at = started
    m.processing_time = proc
    m.error_count = 0
    with pytest.raises(ValueError):
        m.lines_received = -1
    with pytest.raises(ValueError):
        m.lines_forwarded = -2
    with pytest.raises(ValueError):
        m.error_count = -3
    with pytest.raises(ValueError):
        m.processing_time = 123  # not a timedelta
    with pytest.raises(ValueError):
        m.started_at = "not a datetime"  # not a datetime
