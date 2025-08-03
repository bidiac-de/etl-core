import pytest
from src.receivers.data_operations_receivers.filter_receiver import FilterReceiver
from src.components.data_operations.comparison_rule import ComparisonRule
from src.metrics.component_metrics.data_operations_metrics.data_operations_metrics import FilterMetrics
from datetime import datetime, timedelta


@pytest.fixture
def receiver():
    return FilterReceiver()


@pytest.fixture
def metrics():
    return FilterMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0
    )


@pytest.fixture
def rule():
    return ComparisonRule(column="age", operator=">", value=18)


def test_process_row_passes(receiver, metrics, rule):
    """Row matches rule → should be returned and metrics updated."""
    row = {"name": "Alice", "age": 20}
    result = receiver.process_row(metrics, row, rule)

    assert result == row
    assert metrics.lines_received == 1
    assert metrics.lines_forwarded == 1
    assert metrics.lines_dismissed == 0


def test_process_row_fails(receiver, metrics, rule):
    """Row does not match rule → should return {} and update dismissed."""
    row = {"name": "Bob", "age": 16}
    result = receiver.process_row(metrics, row, rule)

    assert result == {}
    assert metrics.lines_received == 1
    assert metrics.lines_forwarded == 0
    assert metrics.lines_dismissed == 1


def test_process_bulk(receiver, metrics, rule):
    """Multiple rows → only matching rows should pass."""
    data = [
        {"name": "Alice", "age": 20},   # matches
        {"name": "Bob", "age": 16},     # fails
        {"name": "Charlie", "age": 25}  # matches
    ]
    result = receiver.process_bulk(metrics, data, rule)

    assert result == [
        {"name": "Alice", "age": 20},
        {"name": "Charlie", "age": 25}
    ]
    assert metrics.lines_received == 3
    assert metrics.lines_forwarded == 2
    assert metrics.lines_dismissed == 1


def test_process_bigdata(receiver, metrics, rule):
    """Streaming data → matches should be yielded."""
    chunk_iterable = [
        [{"name": "User1", "age": 17}, {"name": "User2", "age": 19}],
        [{"name": "User3", "age": 20}, {"name": "User4", "age": 15}],
    ]

    result = list(receiver.process_bigdata(metrics, chunk_iterable, rule))

    assert result == [
        {"name": "User2", "age": 19},
        {"name": "User3", "age": 20}
    ]
    assert metrics.lines_received == 4
    assert metrics.lines_forwarded == 2
    assert metrics.lines_dismissed == 2