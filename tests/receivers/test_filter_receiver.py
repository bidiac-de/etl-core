import pytest
from datetime import datetime, timedelta

from src.receivers.data_operations_receivers.filter_receiver import FilterReceiver
from src.components.data_operations.comparison_rule import ComparisonRule
from src.metrics.component_metrics.data_operations_metrics.filter_metrics import FilterMetrics


@pytest.fixture
def metrics():
    return FilterMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
        lines_dismissed=0,
    )


@pytest.fixture
def sample_data():
    return [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 20},
        {"id": 3, "name": "Charlie", "age": 35},
    ]


def test_receiver_row(metrics, sample_data):
    receiver = FilterReceiver()
    rule = ComparisonRule(column="age", operator=">", value=25)

    result = receiver.process_row(metrics, sample_data[0], rule)
    assert result["name"] == "Alice"
    assert metrics.lines_forwarded == 1
    assert metrics.lines_dismissed == 0


def test_receiver_bulk(metrics, sample_data):
    receiver = FilterReceiver()
    rule = ComparisonRule(column="age", operator="<", value=30)

    result = receiver.process_bulk(metrics, sample_data, rule)
    assert len(result) == 1
    assert result[0]["name"] == "Bob"
    assert metrics.lines_forwarded == 1


def test_receiver_bigdata(metrics, sample_data):
    receiver = FilterReceiver()
    rule = ComparisonRule(column="age", operator=">=", value=30)

    chunk_iter = [sample_data]
    result = list(receiver.process_bigdata(metrics, chunk_iter, rule))

    assert len(result) == 2
    names = [r["name"] for r in result]
    assert "Alice" in names and "Charlie" in names
    assert metrics.lines_forwarded == 2