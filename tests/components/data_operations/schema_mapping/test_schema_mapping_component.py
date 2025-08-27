from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from etl_core.components.envelopes import Out
from etl_core.components.data_operations.schema_mapping.schema_mapping_component import (  # noqa: E501
    SchemaMappingComponent,
)
from etl_core.components.data_operations.schema_mapping.mapping_rule import (
    FieldMapping,
)
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)


def _schema_row_fanout_in() -> Schema:
    return Schema(
        fields=[
            FieldDef(
                name="user",
                data_type="object",
                children=[
                    FieldDef(name="id", data_type="integer"),
                    FieldDef(name="name", data_type="string"),
                    FieldDef(
                        name="address",
                        data_type="object",
                        children=[FieldDef(name="city", data_type="string")],
                    ),
                ],
            )
        ]
    )


def _schema_row_fanout_out_a() -> Schema:
    return Schema(
        fields=[
            FieldDef(name="uid", data_type="integer"),
            FieldDef(name="uname", data_type="string"),
        ]
    )


def _schema_row_fanout_out_b() -> Schema:
    return Schema(fields=[FieldDef(name="city", data_type="string")])


def _schema_id_name_in() -> Schema:
    return Schema(
        fields=[
            FieldDef(name="id", data_type="integer"),
            FieldDef(name="name", data_type="string"),
        ]
    )


def _schema_uid_uname_out() -> Schema:
    return Schema(
        fields=[
            FieldDef(name="uid", data_type="integer"),
            FieldDef(name="uname", data_type="string"),
        ]
    )


def _schema_userid_username_out() -> Schema:
    return Schema(
        fields=[
            FieldDef(name="user_id", data_type="integer"),
            FieldDef(name="user_name", data_type="string"),
        ]
    )


@pytest.fixture
def metrics() -> DataOperationsMetrics:
    return DataOperationsMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
        lines_dismissed=0,
    )


@pytest.mark.asyncio
async def test_component_process_row_fanout(metrics: DataOperationsMetrics) -> None:
    comp = SchemaMappingComponent(
        name="MapRow",
        description="row",
        comp_type="schema_mapping",
        extra_input_ports=["in"],
        in_port_schemas={"in": _schema_row_fanout_in()},
        extra_output_ports=["A", "B"],
        out_port_schemas={
            "A": _schema_row_fanout_out_a(),
            "B": _schema_row_fanout_out_b(),
        },
        rules=[
            FieldMapping(
                src_port="in", src_path="user.id", dst_port="A", dst_path="uid"
            ),
            FieldMapping(
                src_port="in", src_path="user.name", dst_port="A", dst_path="uname"
            ),
            FieldMapping(
                src_port="in",
                src_path="user.address.city",
                dst_port="B",
                dst_path="city",
            ),
        ],
    )

    row: Dict[str, Any] = {
        "user": {"id": 1, "name": "Nina", "address": {"city": "Berlin"}},
    }

    outs: List[Out] = []
    async for o in comp.process_row(row=row, metrics=metrics):
        outs.append(o)

    assert {o.port for o in outs} == {"A", "B"}

    by_port = {o.port: o.payload for o in outs}
    assert by_port["A"] == {"uid": 1, "uname": "Nina"}
    assert by_port["B"] == {"city": "Berlin"}

    assert metrics.lines_received == 1
    assert metrics.lines_processed == 2
    assert metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_component_process_bulk_dataframe(metrics: DataOperationsMetrics) -> None:
    comp = SchemaMappingComponent(
        name="MapBulk",
        description="bulk",
        comp_type="schema_mapping",
        extra_input_ports=["in"],
        in_port_schemas={"in": _schema_id_name_in()},
        extra_output_ports=["X"],
        out_port_schemas={"X": _schema_userid_username_out()},
        rules=[
            FieldMapping(
                src_port="in", src_path="id", dst_port="X", dst_path="user_id"
            ),
            FieldMapping(
                src_port="in", src_path="name", dst_port="X", dst_path="user_name"
            ),
        ],
    )

    df = pd.DataFrame([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])

    outs = []
    async for o in comp.process_bulk(df, metrics=metrics):
        outs.append(o)
    assert len(outs) == 1 and outs[0].port == "X"
    out_df = outs[0].payload

    expected = pd.DataFrame({"user_id": [1, 2], "user_name": ["A", "B"]})
    assert_frame_equal(out_df, expected, check_dtype=False)

    assert metrics.lines_received == 2
    assert metrics.lines_processed == 2
    assert metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_component_process_bigdata(metrics: DataOperationsMetrics) -> None:
    comp = SchemaMappingComponent(
        name="MapBig",
        description="big",
        comp_type="schema_mapping",
        extra_input_ports=["in"],
        in_port_schemas={"in": _schema_id_name_in()},
        extra_output_ports=["out"],
        out_port_schemas={"out": _schema_uid_uname_out()},
        rules=[
            FieldMapping(src_port="in", src_path="id", dst_port="out", dst_path="uid"),
            FieldMapping(
                src_port="in", src_path="name", dst_port="out", dst_path="uname"
            ),
        ],
    )

    pdf = pd.DataFrame([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
    ddf = dd.from_pandas(pdf, npartitions=2)

    outs = []
    async for o in comp.process_bigdata(ddf, metrics=metrics):
        outs.append(o)
    assert len(outs) == 1 and outs[0].port == "out"
    out_ddf = outs[0].payload

    got = out_ddf.compute().sort_values(["uid"]).reset_index(drop=True)
    expected = pd.DataFrame({"uid": [1, 2], "uname": ["A", "B"]})
    assert_frame_equal(got, expected, check_dtype=False)

    assert metrics.lines_processed == 2
    assert metrics.lines_forwarded == 2
    assert metrics.lines_received == 2


def test_field_mapping_validator_rejects_empty_segments() -> None:
    with pytest.raises(ValueError):
        FieldMapping(
            src_port="in",
            src_path="a..b",  # invalid: empty segment
            dst_port="out",
            dst_path="x.y",
        )
