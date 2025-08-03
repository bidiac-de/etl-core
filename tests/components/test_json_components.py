import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.components.file_components.json.read_json_component import ReadJSON
from src.components.file_components.json.write_json_component import WriteJSON
from src.components.column_definition import ColumnDefinition, DataType
from src.metrics.component_metrics import ComponentMetrics
from src.strategies.row_strategy import RowExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy


# --- Patch strategies: swallow metrics kwarg and pass component.metrics into process_* ---
@pytest.fixture(autouse=True)
def patch_strategies(monkeypatch):
    def row_exec(self, component, inputs, **kwargs):
        return component.process_row(inputs, metrics=getattr(component, "metrics", None))

    def bulk_exec(self, component, inputs, **kwargs):
        return component.process_bulk(inputs, metrics=getattr(component, "metrics", None))

    monkeypatch.setattr(RowExecutionStrategy, "execute", row_exec, raising=True)
    monkeypatch.setattr(BulkExecutionStrategy, "execute", bulk_exec, raising=True)


# ---------- Fixtures ----------
@pytest.fixture
def metrics() -> ComponentMetrics:
    return ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )

@pytest.fixture
def schema_definition():
    return [
        ColumnDefinition("id", DataType.INTEGER),
        ColumnDefinition("name", DataType.STRING),
    ]

@pytest.fixture
def sample_json_file(tmp_path: Path) -> Path:
    fp = tmp_path / "input.json"
    fp.write_text(json.dumps([
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]), encoding="utf-8")
    return fp


# ---------- ReadJSON via execute() ----------
def test_readjson_execute_bulk(sample_json_file: Path, schema_definition, metrics: ComponentMetrics):
    comp = ReadJSON(
        name="Read bulk",
        description="Read all records",
        comp_type="read_json",
        strategy_type="bulk",
        filepath=sample_json_file,
        schema_definition=schema_definition,
    )
    comp.metrics = metrics

    result = comp.execute(data=None, metrics=metrics)  # metrics kwarg wird von Patch geschluckt
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert set(result.columns) >= {"id", "name"}
    assert result.iloc[0]["name"] == "Alice"


def test_readjson_execute_row(sample_json_file: Path, schema_definition, metrics: ComponentMetrics):
    comp = ReadJSON(
        name="Read row",
        description="Read first record",
        comp_type="read_json",
        strategy_type="row",
        filepath=sample_json_file,
        schema_definition=schema_definition,
    )
    comp.metrics = metrics

    result = comp.execute(data={}, metrics=metrics)
    assert isinstance(result, dict)
    assert result.get("name") == "Alice"


# ---------- WriteJSON via execute() ----------
def test_writejson_execute_bulk_and_readback(tmp_path: Path, schema_definition, metrics: ComponentMetrics):
    out_fp = tmp_path / "out.json"
    out_fp.parent.mkdir(parents=True, exist_ok=True)
    out_fp.touch()  # Validator requires existing file

    writer = WriteJSON(
        name="Write bulk",
        description="Write records as array",
        comp_type="write_json",
        strategy_type="bulk",
        filepath=out_fp,
        schema_definition=schema_definition,
    )
    writer.metrics = metrics

    df = pd.DataFrame([
        {"id": 10, "name": "Charlie"},
        {"id": 11, "name": "Diana"},
    ])

    write_result = writer.execute(data=df, metrics=metrics)
    assert isinstance(write_result, pd.DataFrame)
    assert out_fp.exists()

    reader = ReadJSON(
        name="Read back",
        description="Read written file",
        comp_type="read_json",
        strategy_type="bulk",
        filepath=out_fp,
        schema_definition=schema_definition,
    )
    reader.metrics = metrics

    read_df = reader.execute(data=None, metrics=metrics)
    assert len(read_df) == 2
    assert list(read_df.sort_values("id")["name"]) == ["Charlie", "Diana"]


def test_writejson_execute_row(tmp_path: Path, schema_definition, metrics: ComponentMetrics):
    single_fp = tmp_path / "single.json"
    single_fp.parent.mkdir(parents=True, exist_ok=True)
    single_fp.touch()  # Validator requires existing file

    writer = WriteJSON(
        name="Write single row",
        description="Write first row",
        comp_type="write_json",
        strategy_type="row",
        filepath=single_fp,
        schema_definition=schema_definition,
    )
    writer.metrics = metrics

    row = {"id": 99, "name": "SingleRow"}
    result = writer.execute(data=row, metrics=metrics)
    assert result == row

    content = json.loads(single_fp.read_text(encoding="utf-8"))
    assert isinstance(content, list)
    assert content and content[0]["id"] == 99 and content[0]["name"] == "SingleRow"

