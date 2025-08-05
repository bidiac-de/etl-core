import pytest
from datetime import datetime, timedelta

from src.components.data_operations.filter_component import FilterComponent
from src.components.data_operations.comparison_rule import ComparisonRule
from src.metrics.component_metrics.data_operations_metrics.filter_metrics import FilterMetrics
from src.components.base_component import get_strategy


@pytest.fixture
def sample_data():
    return [
        {"id": 1, "name": "Alice", "age": 30, "country": "DE"},
        {"id": 2, "name": "Bob", "age": 20, "country": "FR"},
        {"id": 3, "name": "Charlie", "age": 35, "country": "US"},
    ]


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


def test_filter_row(sample_data, metrics):
    comp = FilterComponent(
        name="FilterRow",
        description="Filter rows older than 25",
        comp_type="filter",
        schema_definition=[],
        strategy_type="row",
        strategy=get_strategy("row"),
        comparison_rule=ComparisonRule(column="age", operator=">", value=25),
    )
    result = comp.process_row(sample_data[0], metrics)
    assert result["name"] == "Alice"


def test_filter_bulk(sample_data, metrics):
    comp = FilterComponent(
        name="FilterBulk",
        description="Filter only US entries",
        comp_type="filter",
        schema_definition=[],
        strategy_type="bulk",
        strategy=get_strategy("bulk"),
        comparison_rule=ComparisonRule(column="country", operator="==", value="US"),
    )
    result = comp.process_bulk(sample_data, metrics)
    assert len(result) == 1
    assert result[0]["name"] == "Charlie"


def test_filter_nested_rules(sample_data, metrics):
    rule = ComparisonRule(
        logical_operator="AND",
        rules=[
            ComparisonRule(column="age", operator=">", value=25),
            ComparisonRule(
                logical_operator="OR",
                rules=[
                    ComparisonRule(column="country", operator="==", value="DE"),
                    ComparisonRule(column="name", operator="contains", value="Ali"),
                ],
            ),
        ],
    )

    comp = FilterComponent(
        name="FilterNested",
        description="Nested logic filter",
        comp_type="filter",
        schema_definition=[],
        strategy_type="bulk",
        strategy=get_strategy("bulk"),
        comparison_rule=rule,
    )
    result = comp.process_bulk(sample_data, metrics)
    assert len(result) == 1
    assert result[0]["name"] == "Alice"