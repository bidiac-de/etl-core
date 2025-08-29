from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Dict, List, TypeVar

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from etl_core.components.envelopes import InTagged, Out
from etl_core.components.data_operations.schema_mapping.schema_mapping_component import (  # noqa: E501
    SchemaMappingComponent,
)
from etl_core.components.data_operations.schema_mapping.mapping_rule import (
    FieldMapping,
)
from etl_core.components.data_operations.schema_mapping.join_rules import (
    JoinPlan,
    JoinStep,
)
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)

T = TypeVar("T")


async def collect(stream: AsyncIterator[T]) -> List[T]:
    """Materialize an async stream to a list, used for all component calls."""
    return [item async for item in stream]


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

    outs: List[Out] = await collect(comp.process_row(row=row, metrics=metrics))
    assert {o.port for o in outs} == {"A", "B"}

    by_port = {o.port: o.payload for o in outs}
    assert by_port["A"] == {"uid": 1, "uname": "Nina"}
    assert by_port["B"] == {"city": "Berlin"}

    assert metrics.lines_received == 1
    assert metrics.lines_processed == 2
    assert metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_component_process_bulk_dataframe(
    metrics: DataOperationsMetrics,
) -> None:
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

    outs = await collect(comp.process_bulk(df, metrics=metrics))
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

    outs = await collect(comp.process_bigdata(ddf, metrics=metrics))
    assert len(outs) == 1 and outs[0].port == "out"
    out_ddf = outs[0].payload

    got = out_ddf.compute().sort_values(["uid"]).reset_index(drop=True)
    expected = pd.DataFrame({"uid": [1, 2], "uname": ["A", "B"]})
    assert_frame_equal(got, expected, check_dtype=False)

    assert metrics.lines_processed == 2
    assert metrics.lines_forwarded == 2
    assert metrics.lines_received == 2


@pytest.mark.asyncio
async def test_component_multi_input_row_inner_join(
    metrics: DataOperationsMetrics,
) -> None:
    users_schema = Schema(
        fields=[
            FieldDef(
                name="user",
                data_type="object",
                children=[
                    FieldDef(name="id", data_type="integer"),
                    FieldDef(name="name", data_type="string"),
                ],
            )
        ]
    )
    purchases_schema = Schema(
        fields=[
            FieldDef(
                name="purchase",
                data_type="object",
                children=[
                    FieldDef(name="user_id", data_type="integer"),
                    FieldDef(name="city", data_type="string"),
                ],
            )
        ]
    )
    joined_schema = Schema(
        fields=[
            FieldDef(
                name="user",
                data_type="object",
                children=[
                    FieldDef(name="id", data_type="integer"),
                    FieldDef(name="name", data_type="string"),
                ],
            ),
            FieldDef(
                name="purchase",
                data_type="object",
                children=[
                    FieldDef(name="user_id", data_type="integer"),
                    FieldDef(name="city", data_type="string"),
                ],
            ),
        ]
    )

    comp = SchemaMappingComponent(
        name="JoinRow",
        description="row joins",
        comp_type="schema_mapping",
        extra_input_ports=["users", "purchases"],
        in_port_schemas={"users": users_schema, "purchases": purchases_schema},
        extra_output_ports=["joined"],
        out_port_schemas={"joined": joined_schema},
        join_plan=JoinPlan(
            steps=[
                JoinStep(
                    left_port="users",
                    right_port="purchases",
                    left_on="user.id",
                    right_on="purchase.user_id",
                    how="inner",
                    output_port="joined",
                )
            ]
        ),
    )

    outs: List[Out] = []
    outs.extend(
        await collect(
            comp.process_row(
                InTagged("users", {"user": {"id": 1, "name": "Nina"}}),
                metrics=metrics,
            )
        )
    )
    outs.extend(
        await collect(
            comp.process_row(
                InTagged("purchases", {"purchase": {"user_id": 1, "city": "Berlin"}}),
                metrics=metrics,
            )
        )
    )
    outs.extend(
        await collect(comp.process_row(InTagged("users", Ellipsis), metrics=metrics))
    )
    outs.extend(
        await collect(
            comp.process_row(InTagged("purchases", Ellipsis), metrics=metrics)
        )
    )

    assert [o.port for o in outs] == ["joined"]
    merged = outs[0].payload
    assert merged["user"]["id"] == 1
    assert merged["purchase"]["city"] == "Berlin"


@pytest.mark.asyncio
async def test_component_multi_input_bulk_left_join(
    metrics: DataOperationsMetrics,
) -> None:
    left_schema = Schema(
        fields=[
            FieldDef(name="id", data_type="integer"),
            FieldDef(name="name", data_type="string"),
        ]
    )
    right_schema = Schema(
        fields=[
            FieldDef(name="user_id", data_type="integer"),
            FieldDef(name="city", data_type="string"),
        ]
    )
    out_schema = Schema(
        fields=[
            FieldDef(name="id", data_type="integer"),
            FieldDef(name="name", data_type="string"),
            FieldDef(name="city", data_type="string"),
        ]
    )

    comp = SchemaMappingComponent(
        name="JoinBulk",
        description="bulk joins",
        comp_type="schema_mapping",
        extra_input_ports=["L", "R"],
        in_port_schemas={"L": left_schema, "R": right_schema},
        extra_output_ports=["J"],
        out_port_schemas={"J": out_schema},
        join_plan=JoinPlan(
            steps=[
                JoinStep(
                    left_port="L",
                    right_port="R",
                    left_on="id",
                    right_on="user_id",
                    how="left",
                    output_port="J",
                )
            ]
        ),
    )

    outs: List[Out] = []
    outs.extend(
        await collect(
            comp.process_bulk(
                InTagged(
                    "L",
                    pd.DataFrame([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]),
                ),
                metrics=metrics,
            )
        )
    )
    outs.extend(
        await collect(
            comp.process_bulk(
                InTagged("R", pd.DataFrame([{"user_id": 1, "city": "Berlin"}])),
                metrics=metrics,
            )
        )
    )
    outs.extend(
        await collect(comp.process_bulk(InTagged("L", Ellipsis), metrics=metrics))
    )
    outs.extend(
        await collect(comp.process_bulk(InTagged("R", Ellipsis), metrics=metrics))
    )

    assert [o.port for o in outs] == ["J"]
    got = outs[0].payload.reset_index(drop=True)
    expected = pd.DataFrame(
        {"id": [1, 2], "name": ["A", "B"], "city": ["Berlin", None]}
    )
    assert_frame_equal(got, expected, check_dtype=False)


@pytest.mark.asyncio
async def test_component_multi_input_bigdata_outer_join(
    metrics: DataOperationsMetrics,
) -> None:
    left_pdf = pd.DataFrame([{"id": 1}, {"id": 2}])
    right_pdf = pd.DataFrame([{"user_id": 2}, {"user_id": 3}])
    l_ddf = dd.from_pandas(left_pdf, npartitions=2)
    r_ddf = dd.from_pandas(right_pdf, npartitions=2)

    schema_l = Schema(fields=[FieldDef(name="id", data_type="integer")])
    schema_r = Schema(fields=[FieldDef(name="user_id", data_type="integer")])
    schema_out = Schema(
        fields=[
            FieldDef(name="id", data_type="integer"),
            FieldDef(name="user_id", data_type="integer"),
        ]
    )

    comp = SchemaMappingComponent(
        name="JoinBig",
        description="bigdata joins",
        comp_type="schema_mapping",
        extra_input_ports=["L", "R"],
        in_port_schemas={"L": schema_l, "R": schema_r},
        extra_output_ports=["J"],
        out_port_schemas={"J": schema_out},
        join_plan=JoinPlan(
            steps=[
                JoinStep(
                    left_port="L",
                    right_port="R",
                    left_on="id",
                    right_on="user_id",
                    how="outer",
                    output_port="J",
                )
            ]
        ),
    )

    outs: List[Out] = []
    outs.extend(
        await collect(comp.process_bigdata(InTagged("L", l_ddf), metrics=metrics))
    )
    outs.extend(
        await collect(comp.process_bigdata(InTagged("R", r_ddf), metrics=metrics))
    )
    outs.extend(
        await collect(comp.process_bigdata(InTagged("L", Ellipsis), metrics=metrics))
    )
    outs.extend(
        await collect(comp.process_bigdata(InTagged("R", Ellipsis), metrics=metrics))
    )

    assert [o.port for o in outs] == ["J"]
    got = (
        outs[0].payload.compute().sort_values(["id", "user_id"]).reset_index(drop=True)
    )
    expected = pd.DataFrame({"id": [1, 2, None], "user_id": [None, 2, 3]})
    assert_frame_equal(got, expected, check_dtype=False)


@pytest.mark.asyncio
async def test_component_multi_step_chained_joins_row(
    metrics: DataOperationsMetrics,
) -> None:
    schema_a = Schema(
        fields=[
            FieldDef(name="id", data_type="integer"),
            FieldDef(name="name", data_type="string"),
        ]
    )
    schema_b = Schema(
        fields=[
            FieldDef(name="bid", data_type="integer"),
            FieldDef(name="group", data_type="string"),
        ]
    )
    schema_c = Schema(
        fields=[
            FieldDef(name="cid", data_type="integer"),
            FieldDef(name="city", data_type="string"),
        ]
    )

    # Outputs AB and ABC must exist as declared ports
    schema_ab = Schema(fields=[FieldDef(name="id", data_type="integer")])
    schema_abc = Schema(fields=[FieldDef(name="id", data_type="integer")])

    comp = SchemaMappingComponent(
        name="JoinChainRow",
        description="two-step row join",
        comp_type="schema_mapping",
        extra_input_ports=["A", "B", "C"],
        in_port_schemas={"A": schema_a, "B": schema_b, "C": schema_c},
        extra_output_ports=["AB", "ABC"],
        out_port_schemas={"AB": schema_ab, "ABC": schema_abc},
        join_plan=JoinPlan(
            steps=[
                JoinStep(
                    left_port="A",
                    right_port="B",
                    left_on="id",
                    right_on="bid",
                    how="inner",
                    output_port="AB",
                ),
                JoinStep(
                    left_port="AB",
                    right_port="C",
                    left_on="id",
                    right_on="cid",
                    how="left",
                    output_port="ABC",
                ),
            ]
        ),
    )

    outs: List[Out] = []
    outs.extend(
        await collect(
            comp.process_row(InTagged("A", {"id": 1, "name": "A1"}), metrics=metrics)
        )
    )
    outs.extend(
        await collect(
            comp.process_row(InTagged("B", {"bid": 1, "group": "G"}), metrics=metrics)
        )
    )
    outs.extend(
        await collect(
            comp.process_row(InTagged("C", {"cid": 1, "city": "BE"}), metrics=metrics)
        )
    )
    outs.extend(
        await collect(comp.process_row(InTagged("A", Ellipsis), metrics=metrics))
    )
    outs.extend(
        await collect(comp.process_row(InTagged("B", Ellipsis), metrics=metrics))
    )
    outs.extend(
        await collect(comp.process_row(InTagged("C", Ellipsis), metrics=metrics))
    )

    ports = [o.port for o in outs]
    assert set(ports) == {"AB", "ABC"}
    by_port = {o.port: o.payload for o in outs}
    assert by_port["AB"]["id"] == 1
    assert by_port["ABC"]["city"] == "BE"


@pytest.mark.asyncio
async def test_bulk_left_join_large_dataset(
    metrics: DataOperationsMetrics,
) -> None:
    # L: 1000 users, R: only even user_ids have a city
    n = 1000
    left_pdf = pd.DataFrame([{"id": i, "name": f"U{i}"} for i in range(1, n + 1)])
    right_pdf = pd.DataFrame(
        [{"user_id": i, "city": f"C{i}"} for i in range(2, n + 1, 2)]
    )

    left_schema = Schema(
        fields=[
            FieldDef(name="id", data_type="integer"),
            FieldDef(name="name", data_type="string"),
        ]
    )
    right_schema = Schema(
        fields=[
            FieldDef(name="user_id", data_type="integer"),
            FieldDef(name="city", data_type="string"),
        ]
    )
    out_schema = Schema(
        fields=[
            FieldDef(name="id", data_type="integer"),
            FieldDef(name="name", data_type="string"),
            FieldDef(name="city", data_type="string"),
        ]
    )

    comp = SchemaMappingComponent(
        name="JoinBulkLarge",
        description="bulk left join large",
        comp_type="schema_mapping",
        extra_input_ports=["L", "R"],
        in_port_schemas={"L": left_schema, "R": right_schema},
        extra_output_ports=["J"],
        out_port_schemas={"J": out_schema},
        join_plan=JoinPlan(
            steps=[
                JoinStep(
                    left_port="L",
                    right_port="R",
                    left_on="id",
                    right_on="user_id",
                    how="left",
                    output_port="J",
                )
            ]
        ),
    )

    outs: List[Out] = []
    outs += await collect(comp.process_bulk(InTagged("L", left_pdf), metrics=metrics))
    outs += await collect(comp.process_bulk(InTagged("R", right_pdf), metrics=metrics))
    outs += await collect(comp.process_bulk(InTagged("L", Ellipsis), metrics=metrics))
    outs += await collect(comp.process_bulk(InTagged("R", Ellipsis), metrics=metrics))

    assert [o.port for o in outs] == ["J"]

    got = outs[0].payload.reset_index(drop=True)
    # shape: n rows, 3 cols (id, name, city)
    assert got.shape == (n, 3)
    # spot checks
    assert got.loc[0, "id"] == 1 and got.loc[0, "name"] == "U1"
    assert pd.isna(got.loc[0, "city"])  # 1 has no city
    assert got.loc[1, "id"] == 2 and got.loc[1, "city"] == "C2"
    assert got.loc[9, "id"] == 10 and got.loc[9, "city"] == "C10"


@pytest.mark.asyncio
async def test_bigdata_outer_join_large_dataset(
    metrics: DataOperationsMetrics,
) -> None:
    # Outer join: L ids 1..1500, R user_ids 1001..2500
    n_left = 1500
    n_right = 1500
    left_pdf = pd.DataFrame([{"id": i} for i in range(1, n_left + 1)])
    right_pdf = pd.DataFrame([{"user_id": i} for i in range(1001, 1001 + n_right)])

    l_ddf = dd.from_pandas(left_pdf, npartitions=8)
    r_ddf = dd.from_pandas(right_pdf, npartitions=8)

    schema_l = Schema(fields=[FieldDef(name="id", data_type="integer")])
    schema_r = Schema(fields=[FieldDef(name="user_id", data_type="integer")])
    schema_out = Schema(
        fields=[
            FieldDef(name="id", data_type="integer"),
            FieldDef(name="user_id", data_type="integer"),
        ]
    )

    comp = SchemaMappingComponent(
        name="JoinBigOuterLarge",
        description="bigdata outer join large",
        comp_type="schema_mapping",
        extra_input_ports=["L", "R"],
        in_port_schemas={"L": schema_l, "R": schema_r},
        extra_output_ports=["J"],
        out_port_schemas={"J": schema_out},
        join_plan=JoinPlan(
            steps=[
                JoinStep(
                    left_port="L",
                    right_port="R",
                    left_on="id",
                    right_on="user_id",
                    how="outer",
                    output_port="J",
                )
            ]
        ),
    )

    outs: List[Out] = []
    outs += await collect(comp.process_bigdata(InTagged("L", l_ddf), metrics=metrics))
    outs += await collect(comp.process_bigdata(InTagged("R", r_ddf), metrics=metrics))
    outs += await collect(
        comp.process_bigdata(InTagged("L", Ellipsis), metrics=metrics)
    )
    outs += await collect(
        comp.process_bigdata(InTagged("R", Ellipsis), metrics=metrics)
    )

    assert [o.port for o in outs] == ["J"]

    got = (
        outs[0].payload.compute().sort_values(["id", "user_id"]).reset_index(drop=True)
    )

    assert len(got) == 2500

    head = got.head(5)
    assert list(head["id"]) == [1, 2, 3, 4, 5]
    assert all(pd.isna(head["user_id"]))

    mid = got[(got["id"] == 1200) & (got["user_id"] == 1200)]
    assert len(mid) == 1

    tail = got.tail(5)
    assert all(pd.isna(tail["id"]))


@pytest.mark.asyncio
async def test_row_join_nested_to_flat_large(
    metrics: DataOperationsMetrics,
) -> None:
    # Users nested, Purchases flat -> OUT flat (id, name, city) after join
    n = 200
    users_schema = Schema(
        fields=[
            FieldDef(
                name="user",
                data_type="object",
                children=[
                    FieldDef(name="id", data_type="integer"),
                    FieldDef(name="name", data_type="string"),
                ],
            )
        ]
    )
    purchases_schema = Schema(
        fields=[
            FieldDef(name="user_id", data_type="integer"),
            FieldDef(name="city", data_type="string"),
        ]
    )
    out_schema = Schema(
        fields=[
            FieldDef(name="id", data_type="integer"),
            FieldDef(name="name", data_type="string"),
            FieldDef(name="city", data_type="string"),
        ]
    )

    comp = SchemaMappingComponent(
        name="RowJoinNestedToFlatLarge",
        description="row join nested->flat",
        comp_type="schema_mapping",
        extra_input_ports=["users", "purchases"],
        in_port_schemas={"users": users_schema, "purchases": purchases_schema},
        extra_output_ports=["OUT"],
        out_port_schemas={"OUT": out_schema},
        join_plan=JoinPlan(
            steps=[
                JoinStep(
                    left_port="users",
                    right_port="purchases",
                    left_on="user.id",
                    right_on="user_id",
                    how="inner",
                    output_port="OUT",
                )
            ]
        ),
    )

    outs: List[Out] = []
    for i in range(1, n + 1):
        outs += await collect(
            comp.process_row(
                InTagged("users", {"user": {"id": i, "name": f"N{i}"}}),
                metrics=metrics,
            )
        )
    for i in range(2, n + 1, 2):
        outs += await collect(
            comp.process_row(
                InTagged("purchases", {"user_id": i, "city": f"X{i}"}),
                metrics=metrics,
            )
        )

    # flush
    outs += await collect(
        comp.process_row(InTagged("users", Ellipsis), metrics=metrics)
    )
    outs += await collect(
        comp.process_row(InTagged("purchases", Ellipsis), metrics=metrics)
    )

    # component yields one Out per joined row
    assert [o.port for o in outs] == ["OUT"] * (n // 2)

    # nested->flat extraction
    rows = []
    for o in outs:
        d = o.payload
        rows.append(
            {
                "id": d.get("user", {}).get("id"),
                "name": d.get("user", {}).get("name"),
                "city": d.get("city"),
            }
        )
    df = pd.DataFrame(rows).sort_values("id").reset_index(drop=True)

    assert set(df.columns) == {"id", "name", "city"}
    assert len(df) == n // 2
    assert list(df["id"].head(3)) == [2, 4, 6]
    assert list(df["city"].head(3)) == ["X2", "X4", "X6"]


async def test_row_join_flat_to_nested_large(
    metrics: DataOperationsMetrics,
) -> None:
    """
    This test verifies row-mode join with large inputs when driving the component
    via InTagged envelopes. Important nuance: in row join mode, the component
    *merges* input dicts and does NOT apply mapping rules; thus nested output
    exists only if the nested structure is already present on at least one side.
    """

    # Left port: flat rows
    left_schema = Schema(
        fields=[
            FieldDef(name="id", data_type="integer"),
            FieldDef(name="name", data_type="string"),
        ]
    )

    # Right port: partly nested rows (addr.city is nested)
    right_schema = Schema(
        fields=[
            FieldDef(name="user_id", data_type="integer"),
            FieldDef(name="addr.city", data_type="string"),
        ]
    )

    # Out port schema: we expect flat id/name and nested addr.city to be present
    out_schema = Schema(
        fields=[
            FieldDef(name="id", data_type="integer"),
            FieldDef(name="name", data_type="string"),
            FieldDef(name="addr.city", data_type="string"),
        ]
    )

    comp = SchemaMappingComponent(
        name="RowJoinFlatToNested",
        description="row join with envelopes; flat left + nested right",
        comp_type="schema_mapping",
        extra_input_ports=["L", "R"],
        in_port_schemas={"L": left_schema, "R": right_schema},
        extra_output_ports=["OUT"],
        out_port_schemas={"OUT": out_schema},
        # No mapping rules on purpose: join path does not apply them in row mode.
        rules=[],
        join_plan=JoinPlan(
            steps=[
                JoinStep(
                    left_port="L",
                    right_port="R",
                    left_on="id",
                    right_on="user_id",
                    how="inner",
                    output_port="OUT",
                )
            ]
        ),
    )

    assert (
        comp.requires_tagged_input() is True
    ), "component must require envelopes in this mode"

    n = 1000
    # Feed left rows
    for i in range(n):
        row_l = {"id": i, "name": f"user-{i}"}
        async for _ in comp.process_row(InTagged("L", row_l), metrics=metrics):
            pass

    # Feed right rows (nested addr.city)
    for i in range(n):
        row_r = {"user_id": i, "addr": {"city": f"City-{i}"}}
        async for _ in comp.process_row(InTagged("R", row_r), metrics=metrics):
            pass

    # join runs when second close arrives
    async for _ in comp.process_row(InTagged("L", Ellipsis), metrics=metrics):
        # still not ready until R closes as well
        pass

    outs: list[Out] = []
    async for out in comp.process_row(InTagged("R", Ellipsis), metrics=metrics):
        outs.append(out)

    assert outs, "no output received from row join"
    assert all(isinstance(o, Out) for o in outs)
    assert all(o.port == "OUT" for o in outs)
    assert len(outs) == n

    # Spot-check a few rows for merged structure
    sample = [0, n // 2, n - 1]
    for idx in sample:
        payload = outs[idx].payload
        assert isinstance(payload, dict)

        # flat fields from left
        assert payload.get("id") == idx
        assert payload.get("name") == f"user-{idx}"

        # nested field from right
        assert "addr" in payload and isinstance(payload["addr"], dict)
        assert payload["addr"].get("city") == f"City-{idx}"

    @pytest.mark.asyncio
    async def test_bulk_multi_step_left_left_chain(
        metrics: DataOperationsMetrics,
    ) -> None:
        # Two-Step left join chain
        left_schema = Schema(
            fields=[
                FieldDef(name="id", data_type="integer"),
                FieldDef(name="name", data_type="string"),
            ]
        )
        right1_schema = Schema(
            fields=[
                FieldDef(name="user_id", data_type="integer"),
                FieldDef(name="city", data_type="string"),
            ]
        )
        right2_schema = Schema(
            fields=[
                FieldDef(name="user_id", data_type="integer"),
                FieldDef(name="country", data_type="string"),
            ]
        )
        out_schema = Schema(
            fields=[
                FieldDef(name="id", data_type="integer"),
                FieldDef(name="name", data_type="string"),
                FieldDef(name="city", data_type="string"),
                FieldDef(name="country", data_type="string"),
            ]
        )

        comp = SchemaMappingComponent(
            name="BulkChainLeftLeft",
            description="bulk 2-step left-left",
            comp_type="schema_mapping",
            extra_input_ports=["L", "R1", "R2"],
            in_port_schemas={
                "L": left_schema,
                "R1": right1_schema,
                "R2": right2_schema,
            },
            extra_output_ports=["J1", "J"],
            out_port_schemas={"J1": out_schema, "J": out_schema},
            join_plan=JoinPlan(
                steps=[
                    JoinStep(
                        left_port="L",
                        right_port="R1",
                        left_on="id",
                        right_on="user_id",
                        how="left",
                        output_port="J1",
                    ),
                    JoinStep(
                        left_port="J1",
                        right_port="R2",
                        left_on="id",
                        right_on="user_id",
                        how="left",
                        output_port="J",
                    ),
                ]
            ),
        )

        # data: id=1 matches both; id=2 matches only R2; id=3 matches none
        L = pd.DataFrame(
            [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}, {"id": 3, "name": "C"}]
        )
        R1 = pd.DataFrame([{"user_id": 1, "city": "Berlin"}])
        R2 = pd.DataFrame(
            [{"user_id": 1, "country": "DE"}, {"user_id": 2, "country": "DE"}]
        )

        outs: List[Out] = []
        outs += await collect(comp.process_bulk(InTagged("L", L), metrics=metrics))
        outs += await collect(comp.process_bulk(InTagged("R1", R1), metrics=metrics))
        outs += await collect(comp.process_bulk(InTagged("R2", R2), metrics=metrics))
        outs += await collect(
            comp.process_bulk(InTagged("L", Ellipsis), metrics=metrics)
        )
        outs += await collect(
            comp.process_bulk(InTagged("R1", Ellipsis), metrics=metrics)
        )
        outs += await collect(
            comp.process_bulk(InTagged("R2", Ellipsis), metrics=metrics)
        )

        assert [o.port for o in outs] == ["J"]
        got = outs[0].payload.reset_index(drop=True)

        expected = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["A", "B", "C"],
                "city": ["Berlin", None, None],
                "country": ["DE", "DE", None],
            }
        )
        assert_frame_equal(got, expected, check_dtype=False)

    @pytest.mark.asyncio
    async def test_bigdata_multi_step_inner_left_chain(
        metrics: DataOperationsMetrics,
    ) -> None:
        # Two-Step join chain: inner, then left
        a_schema = Schema(fields=[FieldDef(name="id", data_type="integer")])
        b_schema = Schema(
            fields=[
                FieldDef(name="user_id", data_type="integer"),
                FieldDef(name="score", data_type="integer"),
            ]
        )
        c_schema = Schema(
            fields=[
                FieldDef(name="user_id", data_type="integer"),
                FieldDef(name="level", data_type="string"),
            ]
        )
        out_schema_step = Schema(
            fields=[
                FieldDef(name="id", data_type="integer"),
                FieldDef(name="score", data_type="integer"),
            ]
        )
        out_schema_final = Schema(
            fields=[
                FieldDef(name="id", data_type="integer"),
                FieldDef(name="score", data_type="integer"),
                FieldDef(name="level", data_type="string"),
            ]
        )

        comp = SchemaMappingComponent(
            name="BigChainInnerLeft",
            description="bigdata 2-step inner-left",
            comp_type="schema_mapping",
            extra_input_ports=["A", "B", "C"],
            in_port_schemas={"A": a_schema, "B": b_schema, "C": c_schema},
            extra_output_ports=["J1", "J"],
            out_port_schemas={"J1": out_schema_step, "J": out_schema_final},
            join_plan=JoinPlan(
                steps=[
                    JoinStep(
                        left_port="A",
                        right_port="B",
                        left_on="id",
                        right_on="user_id",
                        how="inner",
                        output_port="J1",
                    ),
                    JoinStep(
                        left_port="J1",
                        right_port="C",
                        left_on="id",
                        right_on="user_id",
                        how="left",
                        output_port="J",
                    ),
                ]
            ),
        )

        A = dd.from_pandas(
            pd.DataFrame([{"id": 1}, {"id": 2}, {"id": 3}]), npartitions=2
        )
        B = dd.from_pandas(
            pd.DataFrame(
                [
                    {"user_id": 1, "score": 10},
                    {"user_id": 3, "score": 30},
                    {"user_id": 4, "score": 40},
                ]
            ),
            npartitions=2,
        )
        C = dd.from_pandas(
            pd.DataFrame(
                [{"user_id": 1, "level": "L1"}, {"user_id": 2, "level": "L2"}]
            ),
            npartitions=2,
        )

        outs: List[Out] = []
        outs += await collect(comp.process_bigdata(InTagged("A", A), metrics=metrics))
        outs += await collect(comp.process_bigdata(InTagged("B", B), metrics=metrics))
        outs += await collect(comp.process_bigdata(InTagged("C", C), metrics=metrics))
        outs += await collect(
            comp.process_bigdata(InTagged("A", Ellipsis), metrics=metrics)
        )
        outs += await collect(
            comp.process_bigdata(InTagged("B", Ellipsis), metrics=metrics)
        )
        outs += await collect(
            comp.process_bigdata(InTagged("C", Ellipsis), metrics=metrics)
        )

        assert [o.port for o in outs] == ["J"]
        got = outs[0].payload.compute().sort_values("id").reset_index(drop=True)
        expected = pd.DataFrame(
            {"id": [1, 3], "score": [10, 30], "level": ["L1", None]}
        )
        assert_frame_equal(got, expected, check_dtype=False)

    @pytest.mark.asyncio
    async def test_row_multi_step_nested_inner_then_left(
        metrics: DataOperationsMetrics,
    ) -> None:
        # Two-Step join chain with nested fields, inner, then left
        users_schema = Schema(
            fields=[
                FieldDef(
                    name="user",
                    data_type="object",
                    children=[
                        FieldDef(name="id", data_type="integer"),
                        FieldDef(name="name", data_type="string"),
                    ],
                )
            ]
        )
        purchases_schema = Schema(
            fields=[
                FieldDef(name="user_id", data_type="integer"),
                FieldDef(name="city", data_type="string"),
            ]
        )
        geo_schema = Schema(
            fields=[
                FieldDef(name="city", data_type="string"),
                FieldDef(name="country", data_type="string"),
            ]
        )
        out_schema_step = Schema(
            fields=[
                FieldDef(name="id", data_type="integer"),
                FieldDef(name="name", data_type="string"),
                FieldDef(name="city", data_type="string"),
            ]
        )
        out_schema_final = Schema(
            fields=[
                FieldDef(name="id", data_type="integer"),
                FieldDef(name="name", data_type="string"),
                FieldDef(name="city", data_type="string"),
                FieldDef(name="country", data_type="string"),
            ]
        )

        comp = SchemaMappingComponent(
            name="RowChainNestedInnerLeft",
            description="row 2-step nested->flat->flat",
            comp_type="schema_mapping",
            extra_input_ports=["users", "purchases", "geo"],
            in_port_schemas={
                "users": users_schema,
                "purchases": purchases_schema,
                "geo": geo_schema,
            },
            extra_output_ports=["J1", "OUT"],
            out_port_schemas={"J1": out_schema_step, "OUT": out_schema_final},
            join_plan=JoinPlan(
                steps=[
                    JoinStep(
                        left_port="users",
                        right_port="purchases",
                        left_on="user.id",
                        right_on="user_id",
                        how="inner",
                        output_port="J1",
                    ),
                    JoinStep(
                        left_port="J1",
                        right_port="geo",
                        left_on="city",
                        right_on="city",
                        how="left",
                        output_port="OUT",
                    ),
                ]
            ),
        )

        outs: List[Out] = []
        # users 1..3
        for i in range(1, 4):
            outs += await collect(
                comp.process_row(
                    InTagged("users", {"user": {"id": i, "name": f"N{i}"}}),
                    metrics=metrics,
                )
            )
        # purchases for ids 1 and 3
        outs += await collect(
            comp.process_row(
                InTagged("purchases", {"user_id": 1, "city": "Berlin"}), metrics=metrics
            )
        )
        outs += await collect(
            comp.process_row(
                InTagged("purchases", {"user_id": 3, "city": "Paris"}), metrics=metrics
            )
        )
        # geo only for Berlin
        outs += await collect(
            comp.process_row(
                InTagged("geo", {"city": "Berlin", "country": "DE"}), metrics=metrics
            )
        )

        # flush
        outs += await collect(
            comp.process_row(InTagged("users", Ellipsis), metrics=metrics)
        )
        outs += await collect(
            comp.process_row(InTagged("purchases", Ellipsis), metrics=metrics)
        )
        outs += await collect(
            comp.process_row(InTagged("geo", Ellipsis), metrics=metrics)
        )

        # Step1 matches ids 1 and 3 -> 2 rows; Step2 adds country for Berlin only
        assert [o.port for o in outs] == ["OUT", "OUT"]

        rows = [o.payload for o in outs]
        norm = []
        for d in rows:
            norm.append(
                {
                    "id": d.get("id") or d.get("user", {}).get("id"),
                    "name": d.get("name") or d.get("user", {}).get("name"),
                    "city": d.get("city"),
                    "country": d.get("country"),
                }
            )
        df = pd.DataFrame(norm).sort_values("id").reset_index(drop=True)
        expected = pd.DataFrame(
            {
                "id": [1, 3],
                "name": ["N1", "N3"],
                "city": ["Berlin", "Paris"],
                "country": ["DE", None],
            }
        )
        assert_frame_equal(df, expected, check_dtype=False)

    @pytest.mark.asyncio
    async def test_row_multi_step_flat_inner_then_left_to_nested(
        metrics: DataOperationsMetrics,
    ) -> None:
        # Two-Step join chain with flat fields, inner, then left, --> nested outüut
        left_schema = Schema(
            fields=[
                FieldDef(name="id", data_type="integer"),
                FieldDef(name="name", data_type="string"),
            ]
        )
        right_schema = Schema(
            fields=[
                FieldDef(name="user_id", data_type="integer"),
                FieldDef(name="city", data_type="string"),
            ]
        )
        geo_schema = Schema(
            fields=[
                FieldDef(name="city", data_type="string"),
                FieldDef(name="country", data_type="string"),
            ]
        )
        out_schema_step = Schema(
            fields=[
                FieldDef(name="id", data_type="integer"),
                FieldDef(name="name", data_type="string"),
                FieldDef(name="city", data_type="string"),
            ]
        )
        out_schema_final = Schema(
            fields=[
                FieldDef(
                    name="user",
                    data_type="object",
                    children=[
                        FieldDef(name="id", data_type="integer"),
                        FieldDef(name="name", data_type="string"),
                    ],
                ),
                FieldDef(
                    name="address",
                    data_type="object",
                    children=[
                        FieldDef(name="city", data_type="string"),
                        FieldDef(name="country", data_type="string"),
                    ],
                ),
            ]
        )

        comp = SchemaMappingComponent(
            name="RowChainFlatToNested",
            description="row 2-step flat->nested",
            comp_type="schema_mapping",
            extra_input_ports=["L", "R", "Geo"],
            in_port_schemas={"L": left_schema, "R": right_schema, "Geo": geo_schema},
            extra_output_ports=["J1", "OUT"],
            out_port_schemas={"J1": out_schema_step, "OUT": out_schema_final},
            join_plan=JoinPlan(
                steps=[
                    JoinStep(
                        left_port="L",
                        right_port="R",
                        left_on="id",
                        right_on="user_id",
                        how="inner",
                        output_port="J1",
                    ),
                    JoinStep(
                        left_port="J1",
                        right_port="Geo",
                        left_on="city",
                        right_on="city",
                        how="left",
                        output_port="OUT",
                    ),
                ]
            ),
        )

        outs: List[Out] = []
        outs += await collect(
            comp.process_row(InTagged("L", {"id": 1, "name": "A"}), metrics=metrics)
        )
        outs += await collect(
            comp.process_row(InTagged("L", {"id": 2, "name": "B"}), metrics=metrics)
        )
        outs += await collect(
            comp.process_row(
                InTagged("R", {"user_id": 1, "city": "Berlin"}), metrics=metrics
            )
        )
        outs += await collect(
            comp.process_row(
                InTagged("Geo", {"city": "Berlin", "country": "DE"}), metrics=metrics
            )
        )

        # flush
        outs += await collect(
            comp.process_row(InTagged("L", Ellipsis), metrics=metrics)
        )
        outs += await collect(
            comp.process_row(InTagged("R", Ellipsis), metrics=metrics)
        )
        outs += await collect(
            comp.process_row(InTagged("Geo", Ellipsis), metrics=metrics)
        )

        # Only id=1 joins in step1, step2 enriches with country
        assert [o.port for o in outs] == ["OUT"]

        payload = outs[0].payload
        assert "user" in payload and "address" in payload
