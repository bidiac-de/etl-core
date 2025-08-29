from __future__ import annotations

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from etl_core.components.envelopes import Out
from etl_core.components.data_operations.type_conversion.type_conversion_component import (
    TypeConversionComponent,
    TypeConversionRuleModel,
)
from etl_core.components.wiring.column_definition import DataType, FieldDef
from etl_core.components.wiring.schema import Schema
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.data_operations_receivers.type_conversion.type_conversion_helper import (
    OnError,
    SchemaValidationError,
)


@pytest.mark.asyncio
async def test_process_row__nested_array_star__to_float() -> None:
    comp = (
        TypeConversionComponent(
            name="row_nested",
            description="row",
            comp_type="type_conversion",
            rules=[
                TypeConversionRuleModel(
                    column_path="payload.items.*.price",
                    target=DataType.FLOAT,
                )
            ],
        )
        ._build_objects()
    )

    comp.in_port_schemas["in"] = Schema(
        fields=[FieldDef(name="payload", data_type=DataType.OBJECT, nullable=True)]
    )
    comp.ensure_schemas_for_used_ports({"in": 1}, {"out": 1})

    row = {
        "payload": {
            "items": [{"price": "1.5"}, {"price": "2"}, {"price": None}],
        }
    }
    metrics = ComponentMetrics()

    outs = [o async for o in comp.process_row(row, metrics)]
    assert len(outs) == 1 and isinstance(outs[0], Out)
    out_row = outs[0].payload
    prices = [it["price"] for it in out_row["payload"]["items"]]
    assert prices == [1.5, 2.0, None]

    assert metrics.lines_received == 1
    assert metrics.lines_forwarded == 1


@pytest.mark.asyncio
async def test_process_bulk__top_level_cast__int_nullable_ok() -> None:
    comp = (
        TypeConversionComponent(
            name="bulk_cast",
            description="bulk",
            comp_type="type_conversion",
            rules=[TypeConversionRuleModel(column_path="age", target=DataType.INTEGER)],
        )
        ._build_objects()
    )
    comp.in_port_schemas["in"] = Schema(
        fields=[FieldDef(name="age", data_type=DataType.STRING, nullable=True)]
    )
    comp.ensure_schemas_for_used_ports({"in": 1}, {"out": 1})

    df = pd.DataFrame({"age": ["1", "2", None, "3"]})
    metrics = ComponentMetrics()

    outs = [o async for o in comp.process_bulk(df, metrics)]
    assert len(outs) == 1
    out_df = outs[0].payload

    expected = (
        pd.DataFrame({"age": [1, 2, pd.NA, 3]})
        .astype({"age": "Int64"})
        .reset_index(drop=True)
    )
    got = out_df.astype({"age": "Int64"}).reset_index(drop=True)
    assert_frame_equal(got, expected, check_dtype=False)

    assert metrics.lines_received == 4
    assert metrics.lines_forwarded == 4


@pytest.mark.asyncio
async def test_process_bulk__on_error_null__bad_to_na() -> None:
    comp = (
        TypeConversionComponent(
            name="bulk_cast_null",
            description="bulk",
            comp_type="type_conversion",
            rules=[
                TypeConversionRuleModel(
                    column_path="age",
                    target=DataType.INTEGER,
                    on_error=OnError.NULL,
                )
            ],
        )
        ._build_objects()
    )
    comp.in_port_schemas["in"] = Schema(
        fields=[FieldDef(name="age", data_type=DataType.STRING, nullable=True)]
    )
    comp.ensure_schemas_for_used_ports({"in": 1}, {"out": 1})

    df = pd.DataFrame({"age": ["x", "2"]})
    metrics = ComponentMetrics()

    outs = [o async for o in comp.process_bulk(df, metrics)]
    out_df = outs[0].payload
    expected = pd.DataFrame({"age": [pd.NA, 2]}).astype({"age": "Int64"})
    got = out_df.astype({"age": "Int64"})
    assert_frame_equal(got.reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False)  # noqa: E501

    assert metrics.lines_received == 2
    assert metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_process_bulk__on_error_raise__throws() -> None:
    comp = (
        TypeConversionComponent(
            name="bulk_cast_raise",
            description="bulk",
            comp_type="type_conversion",
            rules=[TypeConversionRuleModel(column_path="age", target=DataType.INTEGER)],
        )
        ._build_objects()
    )
    comp.in_port_schemas["in"] = Schema(
        fields=[FieldDef(name="age", data_type=DataType.STRING, nullable=True)]
    )
    comp.ensure_schemas_for_used_ports({"in": 1}, {"out": 1})

    df = pd.DataFrame({"age": ["ok", "bad"]})
    metrics = ComponentMetrics()

    with pytest.raises(ValueError):
        _ = [o async for o in comp.process_bulk(df, metrics)]


@pytest.mark.asyncio
async def test_process_bigdata__top_level_cast__boolean_skip() -> None:
    comp = (
        TypeConversionComponent(
            name="bigdata_bool",
            description="bigdata",
            comp_type="type_conversion",
            rules=[
                TypeConversionRuleModel(
                    column_path="flag",
                    target=DataType.BOOLEAN,
                    on_error=OnError.SKIP,
                )
            ],
        )
        ._build_objects()
    )
    comp.in_port_schemas["in"] = Schema(
        fields=[FieldDef(name="flag", data_type=DataType.STRING, nullable=True)]
    )
    comp.ensure_schemas_for_used_ports({"in": 1}, {"out": 1})

    pdf = pd.DataFrame({"flag": ["true", "0", "weird"]})
    ddf = dd.from_pandas(pdf, npartitions=2)
    metrics = ComponentMetrics()

    outs = [o async for o in comp.process_bigdata(ddf, metrics)]
    out_ddf = outs[0].payload
    got = out_ddf.compute()

    assert list(got["flag"]) == [True, False, "weird"]
    assert metrics.lines_received >= 3
    assert metrics.lines_forwarded >= 3


@pytest.mark.asyncio
async def test_process_bulk__schema_missing_column__raises() -> None:
    comp = (
        TypeConversionComponent(
            name="schema_check",
            description="bulk",
            comp_type="type_conversion",
            rules=[TypeConversionRuleModel(column_path="age", target=DataType.INTEGER)],
        )
        ._build_objects()
    )
    comp.in_port_schemas["in"] = Schema(
        fields=[FieldDef(name="age", data_type=DataType.STRING, nullable=True)]
    )
    comp.ensure_schemas_for_used_ports({"in": 1}, {"out": 1})

    df = pd.DataFrame({"other": ["1"]})
    metrics = ComponentMetrics()

    with pytest.raises(SchemaValidationError):
        _ = [o async for o in comp.process_bulk(df, metrics)]