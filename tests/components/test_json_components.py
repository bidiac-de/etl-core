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


DATA_DIR = Path(__file__).resolve().parent / "data"
PEOPLE_JSON = DATA_DIR / "testdata.json"
PEOPLE_JSONL = DATA_DIR / "testdata.jsonl"
EXTRA_MISSING_JSON = DATA_DIR / "testdata_extra_missing.json"
MIXED_TYPES_JSON = DATA_DIR / "testdata_mixed_types.json"
NESTED_JSON = DATA_DIR / "testdata_nested.json"
BAD_JSON = DATA_DIR / "testdata_bad.json"
BAD_LINE_JSONL = DATA_DIR / "testdata_bad_line.jsonl"
MIXED_SCHEMA_JSONL = DATA_DIR / "testdata_mixed_schema.jsonl"


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
        ColumnDefinition(name="id", data_type=DataType.INTEGER),
        ColumnDefinition(name="name", data_type=DataType.STRING),
    ]


@pytest.fixture
def records_std() -> list[dict]:
    assert PEOPLE_JSON.exists(), f"Missing test data file: {PEOPLE_JSON}"
    return json.loads(PEOPLE_JSON.read_text(encoding="utf-8"))


@pytest.fixture
def sample_json_file() -> Path:
    assert PEOPLE_JSON.exists(), f"Missing test data file: {PEOPLE_JSON}"
    return PEOPLE_JSON


@pytest.fixture
def sample_jsonl_file() -> Path:
    assert PEOPLE_JSONL.exists(), f"Missing test data file: {PEOPLE_JSONL}"
    return PEOPLE_JSONL


def test_readjson_row(sample_json_file: Path, schema_definition, metrics: ComponentMetrics, records_std):
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
    assert result == records_std[0]


def test_readjson_bulk(sample_json_file: Path, schema_definition, metrics: ComponentMetrics, records_std):
    comp = ReadJSON(
        name="Read bulk",
        description="Read all records",
        comp_type="read_json",
        strategy_type="bulk",
        filepath=sample_json_file,
        schema_definition=schema_definition,
    )
    comp.metrics = metrics

    df = comp.execute(data=None, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(records_std)
    assert list(df.sort_values("id")["name"]) == [r["name"] for r in sorted(records_std, key=lambda x: x["id"])]


def test_readjson_bigdata(sample_jsonl_file: Path, schema_definition, metrics: ComponentMetrics, records_std):
    """read_bigdata via execute() liest NDJSON (.jsonl) in ein Dask-DataFrame."""
    comp = ReadJSON(
        name="Read bigdata",
        description="Read NDJSON with Dask",
        comp_type="read_json",
        strategy_type="bigdata",
        filepath=sample_jsonl_file,
        schema_definition=schema_definition,
    )
    comp.metrics = metrics

    ddf = comp.execute(data=None, metrics=metrics)
    assert isinstance(ddf, dd.DataFrame)
    df = ddf.compute().sort_values("id")
    assert list(df["name"]) == [r["name"] for r in sorted(records_std, key=lambda x: x["id"])]


def test_writejson_row(tmp_path: Path, schema_definition, metrics: ComponentMetrics, records_std):
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

    row = records_std[0]
    result = writer.execute(data=row, metrics=metrics)
    assert result == row

    content = json.loads(single_fp.read_text(encoding="utf-8"))
    assert isinstance(content, list) and content and content[0] == row


def test_writejson_bulk(tmp_path: Path, schema_definition, metrics: ComponentMetrics, records_std):
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

    df_in = pd.DataFrame(records_std)
    write_result = writer.execute(data=df_in, metrics=metrics)
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

    df_out = reader.execute(data=None, metrics=metrics).sort_values("id")
    assert list(df_out["name"]) == [r["name"] for r in sorted(records_std, key=lambda x: x["id"])]


def test_writejson_bigdata(tmp_path: Path, schema_definition, metrics: ComponentMetrics, records_std):
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
    assert list(df_out["name"]) == [r["name"] for r in sorted(records_std, key=lambda x: x["id"])]

def test_readjson_bulk_extra_missing(schema_definition, metrics: ComponentMetrics):
    """Extra fields + missing fields -> DataFrame with union of columns and NaNs."""
    assert EXTRA_MISSING_JSON.exists(), f"Missing {EXTRA_MISSING_JSON}"
    comp = ReadJSON(
        name="read extra/missing",
        description="extra + missing fields",
        comp_type="read_json",
        strategy_type="bulk",
        filepath=EXTRA_MISSING_JSON,
        schema_definition=schema_definition,
    )
    comp.metrics = metrics
    df = comp.execute(data=None, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert {"id", "name"}.issubset(df.columns)
    assert {"age", "city", "nickname"}.issubset(df.columns)
    assert df.loc[df["name"] == "Bob", "age"].isna().all()

def test_readjson_bulk_mixed_types(schema_definition, metrics: ComponentMetrics):
    """Mixed data types in a column -> usually object/NaN, no crash."""
    assert MIXED_TYPES_JSON.exists(), f"Missing {MIXED_TYPES_JSON}"
    comp = ReadJSON(
        name="read mixed types",
        description="mixed numeric/string/null",
        comp_type="read_json",
        strategy_type="bulk",
        filepath=MIXED_TYPES_JSON,
        schema_definition=schema_definition,
    )
    comp.metrics = metrics
    df = comp.execute(data=None, metrics=metrics)
    assert "score" in df.columns
    pd.to_numeric(df["score"], errors="coerce")

def test_readjson_bulk_nested(schema_definition, metrics: ComponentMetrics):
    """Nested objects -> column with dict/None, no flattening out-of-the-box."""
    assert NESTED_JSON.exists(), f"Missing {NESTED_JSON}"
    comp = ReadJSON(
        name="read nested",
        description="nested objects kept as dict",
        comp_type="read_json",
        strategy_type="bulk",
        filepath=NESTED_JSON,
        schema_definition=schema_definition,
    )
    comp.metrics = metrics
    df = comp.execute(data=None, metrics=metrics)
    assert "addr" in df.columns
    assert isinstance(df.iloc[0]["addr"], dict) or df.iloc[0]["addr"] is None


import pytest as _pytest

def test_readjson_bulk_malformed_raises(schema_definition, metrics: ComponentMetrics):
    """Corrupted JSON file -> parser error expected."""
    assert BAD_JSON.exists(), f"Missing {BAD_JSON}"
    comp = ReadJSON(
        name="read bad",
        description="malformed json",
        comp_type="read_json",
        strategy_type="bulk",
        filepath=BAD_JSON,
        schema_definition=schema_definition,
    )
    comp.metrics = metrics
    with _pytest.raises(Exception):
        _ = comp.execute(data=None, metrics=metrics)

def test_readjson_ndjson_bad_line_raises(schema_definition, metrics: ComponentMetrics):
    """NDJSON with one corrupted line -> error expected."""
    assert BAD_LINE_JSONL.exists(), f"Missing {BAD_LINE_JSONL}"
    comp = ReadJSON(
        name="read ndjson bad line",
        description="bad line in ndjson",
        comp_type="read_json",
        strategy_type="bulk",
        filepath=BAD_LINE_JSONL,
        schema_definition=schema_definition,
    )
    comp.metrics = metrics
    with _pytest.raises(Exception):
        _ = comp.execute(data=None, metrics=metrics)

def test_readjson_ndjson_mixed_schema(schema_definition, metrics: ComponentMetrics):
    """NDJSON with varying schema -> union of columns, NaNs possible."""
    assert MIXED_SCHEMA_JSONL.exists(), f"Missing {MIXED_SCHEMA_JSONL}"
    comp = ReadJSON(
        name="read ndjson mixed schema",
        description="mixed schema ndjson",
        comp_type="read_json",
        strategy_type="bulk",
        filepath=MIXED_SCHEMA_JSONL,
        schema_definition=schema_definition,
    )
    comp.metrics = metrics
    df = comp.execute(data=None, metrics=metrics)
    assert set(df.columns) >= {"id", "name", "nickname", "active"}