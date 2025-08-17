import pytest
import pandas as pd
import dask.dataframe as dd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List

from src.components.file_components.excel.read_excel import ReadExcel
from src.components.file_components.excel.write_excel import WriteExcel
from src.components.column_definition import ColumnDefinition, DataType
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.strategies.row_strategy import RowExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy
from src.strategies.bigdata_strategy import BigDataExecutionStrategy
from src.components import Schema


DATA_DIR = Path(__file__).parent / "data"
VALID_XLSX = DATA_DIR / "test_data.xlsx"
MISSING_VALUES_XLSX = DATA_DIR / "test_data_missing.xlsx"
SCHEMA_MISMATCH_XLSX = DATA_DIR / "test_data_schema.xlsx"
WRONG_TYPES_XLSX = DATA_DIR / "test_data_types.xlsx"


def build_minimal_schema() -> Schema:
    return Schema(
        columns=[
            ColumnDefinition(name="id", data_type=DataType.STRING),
            ColumnDefinition(name="name", data_type=DataType.STRING),
        ]
    )


@pytest.fixture(autouse=True)
def patch_strategies(monkeypatch):
    async def _collect_or_return(x):
        if hasattr(x, "__aiter__"):
            items = []
            async for item in x:
                items.append(item)
            return items
        if hasattr(x, "__await__"):
            return await x
        return x

    async def _call_with_fallback(fn, payload, metrics):
        try:
            return await _collect_or_return(fn(payload, metrics=metrics))
        except TypeError as e:
            if "multiple values for argument 'metrics'" not in str(e):
                raise
            return await _collect_or_return(fn(metrics, payload))

    async def row_exec(self, component, payload, metrics):
        return await _call_with_fallback(component.process_row, payload, metrics)

    async def bulk_exec(self, component, payload, metrics):
        return await _call_with_fallback(component.process_bulk, payload, metrics)

    async def bigdata_exec(self, component, payload, metrics):
        return await _call_with_fallback(component.process_bigdata, payload, metrics)

    from src.strategies.row_strategy import RowExecutionStrategy as _Row
    from src.strategies.bulk_strategy import BulkExecutionStrategy as _Bulk
    from src.strategies.bigdata_strategy import BigDataExecutionStrategy as _Big

    monkeypatch.setattr(_Row, "execute", row_exec, raising=True)
    monkeypatch.setattr(_Bulk, "execute", bulk_exec, raising=True)
    monkeypatch.setattr(_Big, "execute", bigdata_exec, raising=True)


@pytest.fixture
def schema_definition():
    return [
        ColumnDefinition(name="id", data_type=DataType.STRING),
        ColumnDefinition(name="name", data_type=DataType.STRING),
    ]


@pytest.fixture
def metrics():
    return ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )


@pytest.mark.asyncio
async def test_readexcel_valid_bulk(schema_definition, metrics):
    comp = ReadExcel(
        name="ReadExcel_Bulk_Valid",
        description="Valid Excel file",
        comp_type="read_excel",
        filepath=VALID_XLSX,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    res = await comp.execute(payload=None, metrics=metrics)
    df = res[0] if isinstance(res, list) else res
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 1
    assert {"id", "name"}.issubset(set(df.columns))


@pytest.mark.asyncio
async def test_readexcel_missing_values_bulk(schema_definition, metrics):
    comp = ReadExcel(
        name="ReadExcel_Bulk_Missing",
        description="Missing values",
        comp_type="read_excel",
        filepath=MISSING_VALUES_XLSX,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    res = await comp.execute(payload=None, metrics=metrics)
    df = res[0] if isinstance(res, list) else res
    assert isinstance(df, pd.DataFrame)
    assert df.isna().any().any()


@pytest.mark.asyncio
async def test_readexcel_schema_mismatch_bulk(schema_definition, metrics):
    comp = ReadExcel(
        name="ReadExcel_Bulk_Schema",
        description="Schema mismatch",
        comp_type="read_excel",
        filepath=SCHEMA_MISMATCH_XLSX,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    res = await comp.execute(payload=None, metrics=metrics)
    df = res[0] if isinstance(res, list) else res
    expected_cols = {col.name for col in schema_definition}
    actual_cols = set(df.columns)
    assert expected_cols != actual_cols


@pytest.mark.asyncio
async def test_readexcel_wrong_types_bulk(schema_definition, metrics):
    comp = ReadExcel(
        name="ReadExcel_Bulk_WrongTypes",
        description="Wrong data types",
        comp_type="read_excel",
        filepath=WRONG_TYPES_XLSX,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    res = await comp.execute(payload=None, metrics=metrics)
    df = res[0] if isinstance(res, list) else res
    assert isinstance(df, pd.DataFrame)
    assert df["id"].dtype == object


@pytest.mark.asyncio
async def test_readexcel_row_streaming(schema_definition, metrics):
    comp = ReadExcel(
        name="ReadExcel_Row_Stream",
        description="Row streaming",
        comp_type="read_excel",
        filepath=VALID_XLSX,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = RowExecutionStrategy()

    rows: List[Dict[str, Any]] = await comp.execute(payload=None, metrics=metrics)
    assert isinstance(rows, list)
    assert len(rows) >= 1
    assert {"id", "name"}.issubset(set(rows[0].keys()))


@pytest.mark.asyncio
async def test_readexcel_bigdata(schema_definition, metrics):
    comp = ReadExcel(
        name="ReadExcel_BigData",
        description="Read Excel with Dask",
        comp_type="read_excel",
        filepath=VALID_XLSX,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BigDataExecutionStrategy()

    res = await comp.execute(payload=None, metrics=metrics)
    ddf_res = res[0] if isinstance(res, list) else res
    assert isinstance(ddf_res, dd.DataFrame)
    df = ddf_res.compute()
    assert len(df) >= 1
    assert {"id", "name"}.issubset(set(df.columns))



@pytest.mark.asyncio
async def test_writeexcel_row(tmp_path: Path, schema_definition, metrics):
    out_fp = tmp_path / "single.xlsx"

    comp = WriteExcel(
        name="WriteExcel_Row",
        description="Write single row",
        comp_type="write_excel",
        filepath=out_fp,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = RowExecutionStrategy()

    row = {"id": "1", "name": "Zoe"}
    result = await comp.execute(payload=row, metrics=metrics)
    if isinstance(result, list):
        assert len(result) == 1
        result = result[0]
    assert result == row

    reader = ReadExcel(
        name="ReadBack_Row",
        description="Read back written single",
        comp_type="read_excel",
        filepath=out_fp,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    reader.strategy = BulkExecutionStrategy()
    res = await reader.execute(payload=None, metrics=metrics)
    df = res[0] if isinstance(res, list) else res
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["name"] == "Zoe"


@pytest.mark.asyncio
async def test_writeexcel_bulk(tmp_path: Path, schema_definition, metrics):
    out_fp = tmp_path / "bulk.xlsx"

    comp = WriteExcel(
        name="WriteExcel_Bulk",
        description="Write multiple rows",
        comp_type="write_excel",
        filepath=out_fp,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    data = [
        {"id": "1", "name": "A"},
        {"id": "2", "name": "B"},
        {"id": "3", "name": "C"},
    ]

    res = await comp.execute(payload=data, metrics=metrics)

    if isinstance(res, list) and len(res) == 1:
        res = res[0]

    if isinstance(res, pd.DataFrame):
        assert len(res) == 3
        written_df = res
    elif isinstance(res, list):
        assert len(res) == 3
        written_df = pd.DataFrame(res)
    else:
        raise AssertionError(f"Unexpected result type: {type(res)!r}")

    assert out_fp.exists()

    reader = ReadExcel(
        name="ReadBack_Bulk",
        description="Read back bulk",
        comp_type="read_excel",
        filepath=out_fp,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    reader.strategy = BulkExecutionStrategy()
    df_res = await reader.execute(payload=None, metrics=metrics)
    df = df_res[0] if isinstance(df_res, list) else df_res

    assert list(df.sort_values("id")["name"]) == ["A", "B", "C"]
    assert list(written_df.sort_values("id")["name"]) == ["A", "B", "C"]


@pytest.mark.asyncio
async def test_writeexcel_bigdata(tmp_path: Path, schema_definition, metrics):
    out_fp = tmp_path / "big.xlsx"

    comp = WriteExcel(
        name="WriteExcel_BigData",
        description="Write Dask DataFrame",
        comp_type="write_excel",
        filepath=out_fp,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BigDataExecutionStrategy()

    ddf_in = dd.from_pandas(
        pd.DataFrame(
            [
                {"id": "10", "name": "Nina"},
                {"id": "11", "name": "Omar"},
            ]
        ),
        npartitions=2,
    )

    result = await comp.execute(payload=ddf_in, metrics=metrics)
    ddf_out = result[0] if isinstance(result, list) else result
    assert isinstance(ddf_out, dd.DataFrame)
    assert out_fp.exists()

    reader = ReadExcel(
        name="ReadBack_BigData",
        description="Read back big",
        comp_type="read_excel",
        filepath=out_fp,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    reader.strategy = BulkExecutionStrategy()
    read_res = await reader.execute(payload=None, metrics=metrics)
    df = read_res[0] if isinstance(read_res, list) else read_res
    assert list(df.sort_values("id")["name"]) == ["Nina", "Omar"]

@pytest.mark.asyncio
async def test_writeexcel_bulk_xlsm(tmp_path: Path, schema_definition, metrics):
    out_fp = tmp_path / "bulk.xlsm"
    comp = WriteExcel(
        name="WriteExcel_Bulk_Xlsm",
        description="Write multiple rows (xlsm)",
        comp_type="write_excel",
        filepath=out_fp,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()
    data = [{"id": "1", "name": "A"}, {"id": "2", "name": "B"}]
    res = await comp.execute(payload=data, metrics=metrics)
    if isinstance(res, list) and len(res) == 1:
        res = res[0]
    assert (isinstance(res, list) and len(res) == 2) or (
            isinstance(res, pd.DataFrame) and len(res) == 2
    )
    assert out_fp.exists()
    reader = ReadExcel(
        name="ReadBack_Bulk_Xlsm",
        description="Read back xlsm",
        comp_type="read_excel",
        filepath=out_fp,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    reader.strategy = BulkExecutionStrategy()
    df_res = await reader.execute(payload=None, metrics=metrics)
    df = df_res[0] if isinstance(df_res, list) else df_res
    assert list(df.sort_values("id")["name"]) == ["A", "B"]