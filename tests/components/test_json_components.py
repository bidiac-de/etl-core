import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
import dask.dataframe as dd
from src.strategies.bigdata_strategy import BigDataExecutionStrategy

from src.components.file_components.json.read_json_component import ReadJSON
from src.components.file_components.json.write_json_component import WriteJSON
from src.components.column_definition import ColumnDefinition, DataType
from src.metrics.component_metrics import ComponentMetrics
from src.strategies.row_strategy import RowExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy


records_std = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
    {"id": 3, "name": "Charlie"},
]

# zentraler testdaten ordner, verschiedene datensätze auch mit fehlern, valeria und ich nehmen aber immer gleiche datensätze

def write_json(path: Path, records):
    path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")

def write_jsonl(path: Path, records):
    path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in records), encoding="utf-8")


@pytest.fixture(autouse=True)
def patch_strategies(monkeypatch):
    def row_exec(self, component, inputs, **kwargs):
        return component.process_row(inputs, metrics=getattr(component, "metrics", None))

    def bulk_exec(self, component, inputs, **kwargs):
        return component.process_bulk(inputs, metrics=getattr(component, "metrics", None))

    def bigdata_exec(self, component, inputs, **kwargs):
        return component.process_bigdata(inputs, metrics=getattr(component, "metrics", None))

    monkeypatch.setattr(RowExecutionStrategy, "execute", row_exec, raising=True)
    monkeypatch.setattr(BulkExecutionStrategy, "execute", bulk_exec, raising=True)
    monkeypatch.setattr(BigDataExecutionStrategy, "execute", bigdata_exec, raising=True)


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
    write_json(fp, records_std)
    return fp


def test_readjson_row(sample_json_file: Path, schema_definition, metrics: ComponentMetrics):
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


def test_readjson_bulk(sample_json_file: Path, schema_definition, metrics: ComponentMetrics):
    comp = ReadJSON(
        name="Read bulk",
        description="Read all records",
        comp_type="read_json",
        strategy_type="bulk",
        filepath=sample_json_file,
        schema_definition=schema_definition,
    )
    comp.metrics = metrics

    result = comp.execute(data=None, metrics=metrics)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3
    assert list(result.sort_values("id")["name"]) == ["Alice", "Bob", "Charlie"]


def test_readjson_bigdata(tmp_path: Path, schema_definition, metrics: ComponentMetrics):
    """read_bigdata via execute() liest NDJSON (.jsonl) in ein Dask-DataFrame."""
    jsonl_path = tmp_path / "stream.jsonl"
    write_jsonl(jsonl_path, records_std)  # gleiche Daten, als NDJSON
    assert jsonl_path.exists()

    comp = ReadJSON(
        name="Read bigdata",
        description="Read NDJSON with Dask",
        comp_type="read_json",
        strategy_type="bigdata",
        filepath=jsonl_path,
        schema_definition=schema_definition,
    )
    comp.metrics = metrics

    ddf = comp.execute(data=None, metrics=metrics)
    assert isinstance(ddf, dd.DataFrame)
    df = ddf.compute().sort_values("id")
    assert list(df["name"]) == ["Alice", "Bob", "Charlie"]


def test_writejson_row(tmp_path: Path, schema_definition, metrics: ComponentMetrics):
    single_fp = tmp_path / "single.json"
    single_fp.parent.mkdir(parents=True, exist_ok=True)
    single_fp.touch()

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


def test_writejson_bulk(tmp_path: Path, schema_definition, metrics: ComponentMetrics):
    out_fp = tmp_path / "out.json"
    out_fp.parent.mkdir(parents=True, exist_ok=True)
    out_fp.touch()

    writer = WriteJSON(
        name="Write bulk",
        description="Write records as array",
        comp_type="write_json",
        strategy_type="bulk",
        filepath=out_fp,
        schema_definition=schema_definition,
    )
    writer.metrics = metrics

    df = pd.DataFrame(records_std)
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

    read_df = reader.execute(data=None, metrics=metrics).sort_values("id")
    assert list(read_df["name"]) == ["Alice", "Bob", "Charlie"]


def test_writejson_bigdata(tmp_path: Path, schema_definition, metrics: ComponentMetrics):
    """write_bigdata via execute() schreibt partitionierte JSONL-Dateien (part-*.json)."""
    out_dir = tmp_path / "big_out"
    out_dir.mkdir(parents=True, exist_ok=True)

    writer = WriteJSON(
        name="Write bigdata",
        description="Write Dask partitions",
        comp_type="write_json",
        strategy_type="bigdata",
        filepath=out_dir,
        schema_definition=schema_definition,
    )
    writer.metrics = metrics

    ddf_in = dd.from_pandas(pd.DataFrame(records_std), npartitions=2)
    res = writer.execute(data=ddf_in, metrics=metrics)
    assert isinstance(res, dd.DataFrame)

    parts = sorted(out_dir.glob("part-*.json"))
    assert parts, "No partition files written."
    for p in parts:
        assert p.is_file() and p.suffix == ".json"

    ddf_out = dd.read_json([str(p) for p in parts], lines=True, blocksize="64MB")
    df_out = ddf_out.compute().sort_values("id")
    assert list(df_out["name"]) == ["Alice", "Bob", "Charlie"]

