from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

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
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)

from tests.helpers import collect_async
from tests.schema_helpers import schema_from_fields, fd


def make_join_component(
    *,
    name: str,
    in_ports: Iterable[str],
    out_ports: Iterable[Tuple[str, Schema]],
    in_port_schemas: Dict[str, Schema],
    steps: List[JoinStep],
    description: str = "",
) -> SchemaMappingComponent:
    """Factory for join components with minimal boilerplate."""
    extra_out_names = [p for p, _ in out_ports]
    out_port_schemas: Dict[str, Schema] = {}
    for p, s in out_ports:
        out_port_schemas[p] = s
    return SchemaMappingComponent(
        name=name,
        description=description or name,
        comp_type="schema_mapping",
        extra_input_ports=list(in_ports),
        in_port_schemas=in_port_schemas,
        extra_output_ports=extra_out_names,
        out_port_schemas=out_port_schemas,
        join_plan=JoinPlan(steps=steps),
    )


# schema definitions for tests


def _schema_row_fanout_in() -> Schema:
    return schema_from_fields(
        [
            fd(
                "user",
                "object",
                children=[
                    fd("id", "integer"),
                    fd("name", "string"),
                    fd("address", "object", children=[fd("city", "string")]),
                ],
            )
        ]
    )


def _schema_row_fanout_out_a() -> Schema:
    return schema_from_fields([fd("uid", "integer"), fd("uname", "string")])


def _schema_row_fanout_out_b() -> Schema:
    return schema_from_fields([fd("city", "string")])


def _schema_id_name_in() -> Schema:
    return schema_from_fields([fd("id", "integer"), fd("name", "string")])


def _schema_uid_uname_out() -> Schema:
    return schema_from_fields([fd("uid", "integer"), fd("uname", "string")])


def _schema_userid_username_out() -> Schema:
    return schema_from_fields([fd("user_id", "integer"), fd("user_name", "string")])


@pytest.mark.asyncio
async def test_component_process_row_fanout(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
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

    outs: List[Out] = await collect_async(
        comp.process_row(row=row, metrics=data_ops_metrics)
    )
    assert {o.port for o in outs} == {"A", "B"}

    by_port = {o.port: o.payload for o in outs}
    assert by_port["A"] == {"uid": 1, "uname": "Nina"}
    assert by_port["B"] == {"city": "Berlin"}

    assert data_ops_metrics.lines_received == 1
    assert data_ops_metrics.lines_processed == 2
    assert data_ops_metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_component_process_bulk_dataframe(
    data_ops_metrics: DataOperationsMetrics,
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

    outs = await collect_async(comp.process_bulk(df, metrics=data_ops_metrics))
    assert len(outs) == 1 and outs[0].port == "X"
    out_df = outs[0].payload

    expected = pd.DataFrame({"user_id": [1, 2], "user_name": ["A", "B"]})
    assert_frame_equal(out_df, expected, check_dtype=False)

    assert data_ops_metrics.lines_received == 2
    assert data_ops_metrics.lines_processed == 2
    assert data_ops_metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_component_process_bigdata(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
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

    outs = await collect_async(comp.process_bigdata(ddf, metrics=data_ops_metrics))
    assert len(outs) == 1 and outs[0].port == "out"
    out_ddf = outs[0].payload

    got = out_ddf.compute().sort_values(["uid"]).reset_index(drop=True)
    expected = pd.DataFrame({"uid": [1, 2], "uname": ["A", "B"]})
    assert_frame_equal(got, expected, check_dtype=False)

    assert data_ops_metrics.lines_processed == 2
    assert data_ops_metrics.lines_forwarded == 2
    assert data_ops_metrics.lines_received == 2


@pytest.mark.asyncio
async def test_component_multi_input_row_inner_join(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    users_schema = schema_from_fields(
        [
            fd(
                "user",
                "object",
                children=[fd("id", "integer"), fd("name", "string")],
            )
        ]
    )
    purchases_schema = schema_from_fields(
        [
            fd(
                "purchase",
                "object",
                children=[fd("user_id", "integer"), fd("city", "string")],
            )
        ]
    )
    joined_schema = schema_from_fields(
        [
            fd(
                "user",
                "object",
                children=[fd("id", "integer"), fd("name", "string")],
            ),
            fd(
                "purchase",
                "object",
                children=[fd("user_id", "integer"), fd("city", "string")],
            ),
        ]
    )

    comp = make_join_component(
        name="JoinRow",
        description="row joins",
        in_ports=["users", "purchases"],
        in_port_schemas={"users": users_schema, "purchases": purchases_schema},
        out_ports=[("joined", joined_schema)],
        steps=[
            JoinStep(
                left_port="users",
                right_port="purchases",
                left_on="user.id",
                right_on="purchase.user_id",
                how="inner",
                output_port="joined",
            )
        ],
    )

    outs: List[Out] = []
    outs += await collect_async(
        comp.process_row(
            InTagged("users", {"user": {"id": 1, "name": "Nina"}}),
            metrics=data_ops_metrics,
        )
    )
    outs += await collect_async(
        comp.process_row(
            InTagged("purchases", {"purchase": {"user_id": 1, "city": "Berlin"}}),
            metrics=data_ops_metrics,
        )
    )
    outs += await collect_async(
        comp.process_row(InTagged("users", Ellipsis), metrics=data_ops_metrics)
    )
    outs += await collect_async(
        comp.process_row(InTagged("purchases", Ellipsis), metrics=data_ops_metrics)
    )

    assert [o.port for o in outs] == ["joined"]
    merged = outs[0].payload
    assert merged["user"]["id"] == 1
    assert merged["purchase"]["city"] == "Berlin"


@pytest.mark.asyncio
async def test_component_multi_input_bulk_left_join(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    left_schema = _schema_id_name_in()
    right_schema = schema_from_fields([fd("user_id", "integer"), fd("city", "string")])
    out_schema = schema_from_fields(
        [fd("id", "integer"), fd("name", "string"), fd("city", "string")]
    )

    comp = make_join_component(
        name="JoinBulk",
        description="bulk joins",
        in_ports=["L", "R"],
        in_port_schemas={"L": left_schema, "R": right_schema},
        out_ports=[("J", out_schema)],
        steps=[
            JoinStep(
                left_port="L",
                right_port="R",
                left_on="id",
                right_on="user_id",
                how="left",
                output_port="J",
            )
        ],
    )

    outs: List[Out] = []
    outs += await collect_async(
        comp.process_bulk(
            InTagged(
                "L", pd.DataFrame([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
            ),  # noqa: E501
            metrics=data_ops_metrics,
        )
    )
    outs += await collect_async(
        comp.process_bulk(
            InTagged("R", pd.DataFrame([{"user_id": 1, "city": "Berlin"}])),
            metrics=data_ops_metrics,
        )
    )
    outs += await collect_async(
        comp.process_bulk(InTagged("L", Ellipsis), metrics=data_ops_metrics)
    )
    outs += await collect_async(
        comp.process_bulk(InTagged("R", Ellipsis), metrics=data_ops_metrics)
    )

    assert [o.port for o in outs] == ["J"]
    got = outs[0].payload.reset_index(drop=True)
    expected = pd.DataFrame(
        {"id": [1, 2], "name": ["A", "B"], "city": ["Berlin", None]}
    )  # noqa: E501
    assert_frame_equal(got, expected, check_dtype=False)


@pytest.mark.asyncio
async def test_component_multi_input_bigdata_outer_join(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    left_pdf = pd.DataFrame([{"id": 1}, {"id": 2}])
    right_pdf = pd.DataFrame([{"user_id": 2}, {"user_id": 3}])
    l_ddf = dd.from_pandas(left_pdf, npartitions=2)
    r_ddf = dd.from_pandas(right_pdf, npartitions=2)

    schema_l = schema_from_fields([fd("id", "integer")])
    schema_r = schema_from_fields([fd("user_id", "integer")])
    schema_out = schema_from_fields([fd("id", "integer"), fd("user_id", "integer")])

    comp = make_join_component(
        name="JoinBig",
        description="bigdata joins",
        in_ports=["L", "R"],
        in_port_schemas={"L": schema_l, "R": schema_r},
        out_ports=[("J", schema_out)],
        steps=[
            JoinStep(
                left_port="L",
                right_port="R",
                left_on="id",
                right_on="user_id",
                how="outer",
                output_port="J",
            )
        ],
    )

    outs: List[Out] = []
    outs += await collect_async(
        comp.process_bigdata(InTagged("L", l_ddf), metrics=data_ops_metrics)
    )
    outs += await collect_async(
        comp.process_bigdata(InTagged("R", r_ddf), metrics=data_ops_metrics)
    )
    outs += await collect_async(
        comp.process_bigdata(InTagged("L", Ellipsis), metrics=data_ops_metrics)
    )
    outs += await collect_async(
        comp.process_bigdata(InTagged("R", Ellipsis), metrics=data_ops_metrics)
    )

    assert [o.port for o in outs] == ["J"]
    got = (
        outs[0].payload.compute().sort_values(["id", "user_id"]).reset_index(drop=True)
    )
    expected = pd.DataFrame({"id": [1, 2, None], "user_id": [None, 2, 3]})
    assert_frame_equal(got, expected, check_dtype=False)


@pytest.mark.asyncio
async def test_component_multi_step_chained_joins_row(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    schema_a = schema_from_fields([fd("id", "integer"), fd("name", "string")])
    schema_b = schema_from_fields([fd("bid", "integer"), fd("group", "string")])
    schema_c = schema_from_fields([fd("cid", "integer"), fd("city", "string")])

    schema_ab = schema_from_fields([fd("id", "integer")])
    schema_abc = schema_from_fields([fd("id", "integer")])

    comp = make_join_component(
        name="JoinChainRow",
        description="two-step row join",
        in_ports=["A", "B", "C"],
        in_port_schemas={"A": schema_a, "B": schema_b, "C": schema_c},
        out_ports=[("AB", schema_ab), ("ABC", schema_abc)],
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
        ],
    )

    outs: List[Out] = []
    outs += await collect_async(
        comp.process_row(
            InTagged("A", {"id": 1, "name": "A1"}), metrics=data_ops_metrics
        )  # noqa: E501
    )
    outs += await collect_async(
        comp.process_row(
            InTagged("B", {"bid": 1, "group": "G"}), metrics=data_ops_metrics
        )  # noqa: E501
    )
    outs += await collect_async(
        comp.process_row(
            InTagged("C", {"cid": 1, "city": "BE"}), metrics=data_ops_metrics
        )  # noqa: E501
    )
    outs += await collect_async(
        comp.process_row(InTagged("A", Ellipsis), metrics=data_ops_metrics)
    )
    outs += await collect_async(
        comp.process_row(InTagged("B", Ellipsis), metrics=data_ops_metrics)
    )
    outs += await collect_async(
        comp.process_row(InTagged("C", Ellipsis), metrics=data_ops_metrics)
    )

    ports = [o.port for o in outs]
    assert set(ports) == {"AB", "ABC"}
    by_port = {o.port: o.payload for o in outs}
    assert by_port["AB"]["id"] == 1
    assert by_port["ABC"]["city"] == "BE"


@pytest.mark.asyncio
async def test_bulk_left_join_large_dataset(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    n = 1000
    left_pdf = pd.DataFrame([{"id": i, "name": f"U{i}"} for i in range(1, n + 1)])
    right_pdf = pd.DataFrame(
        [{"user_id": i, "city": f"C{i}"} for i in range(2, n + 1, 2)]
    )

    left_schema = _schema_id_name_in()
    right_schema = schema_from_fields([fd("user_id", "integer"), fd("city", "string")])
    out_schema = schema_from_fields(
        [fd("id", "integer"), fd("name", "string"), fd("city", "string")]
    )

    comp = make_join_component(
        name="JoinBulkLarge",
        description="bulk left join large",
        in_ports=["L", "R"],
        in_port_schemas={"L": left_schema, "R": right_schema},
        out_ports=[("J", out_schema)],
        steps=[
            JoinStep(
                left_port="L",
                right_port="R",
                left_on="id",
                right_on="user_id",
                how="left",
                output_port="J",
            )
        ],
    )

    outs: List[Out] = []
    outs += await collect_async(
        comp.process_bulk(InTagged("L", left_pdf), metrics=data_ops_metrics)
    )
    outs += await collect_async(
        comp.process_bulk(InTagged("R", right_pdf), metrics=data_ops_metrics)
    )
    outs += await collect_async(
        comp.process_bulk(InTagged("L", Ellipsis), metrics=data_ops_metrics)
    )
    outs += await collect_async(
        comp.process_bulk(InTagged("R", Ellipsis), metrics=data_ops_metrics)
    )

    assert [o.port for o in outs] == ["J"]

    got = outs[0].payload.reset_index(drop=True)
    assert got.shape == (n, 3)
    assert got.loc[0, "id"] == 1 and got.loc[0, "name"] == "U1"
    assert pd.isna(got.loc[0, "city"])
    assert got.loc[1, "id"] == 2 and got.loc[1, "city"] == "C2"
    assert got.loc[9, "id"] == 10 and got.loc[9, "city"] == "C10"


@pytest.mark.asyncio
async def test_bigdata_outer_join_large_dataset(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    n_left = 1500
    n_right = 1500
    left_pdf = pd.DataFrame([{"id": i} for i in range(1, n_left + 1)])
    right_pdf = pd.DataFrame([{"user_id": i} for i in range(1001, 1001 + n_right)])

    l_ddf = dd.from_pandas(left_pdf, npartitions=8)
    r_ddf = dd.from_pandas(right_pdf, npartitions=8)

    schema_l = schema_from_fields([fd("id", "integer")])
    schema_r = schema_from_fields([fd("user_id", "integer")])
    schema_out = schema_from_fields([fd("id", "integer"), fd("user_id", "integer")])

    comp = make_join_component(
        name="JoinBigOuterLarge",
        description="bigdata outer join large",
        in_ports=["L", "R"],
        in_port_schemas={"L": schema_l, "R": schema_r},
        out_ports=[("J", schema_out)],
        steps=[
            JoinStep(
                left_port="L",
                right_port="R",
                left_on="id",
                right_on="user_id",
                how="outer",
                output_port="J",
            )
        ],
    )

    outs: List[Out] = []
    outs += await collect_async(
        comp.process_bigdata(InTagged("L", l_ddf), metrics=data_ops_metrics)
    )
    outs += await collect_async(
        comp.process_bigdata(InTagged("R", r_ddf), metrics=data_ops_metrics)
    )
    outs += await collect_async(
        comp.process_bigdata(InTagged("L", Ellipsis), metrics=data_ops_metrics)
    )
    outs += await collect_async(
        comp.process_bigdata(InTagged("R", Ellipsis), metrics=data_ops_metrics)
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
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    # Users nested, Purchases flat -> OUT flat (id, name, city) after join
    n = 200
    users_schema = schema_from_fields(
        [
            fd(
                "user",
                "object",
                children=[fd("id", "integer"), fd("name", "string")],
            )
        ]
    )
    purchases_schema = _schema_userid_username_out().model_copy(
        update={"fields": [fd("user_id", "integer"), fd("city", "string")]}
    )
    out_schema = schema_from_fields(
        [fd("id", "integer"), fd("name", "string"), fd("city", "string")]
    )

    comp = make_join_component(
        name="RowJoinNestedToFlatLarge",
        description="row join nested->flat",
        in_ports=["users", "purchases"],
        in_port_schemas={"users": users_schema, "purchases": purchases_schema},
        out_ports=[("OUT", out_schema)],
        steps=[
            JoinStep(
                left_port="users",
                right_port="purchases",
                left_on="user.id",
                right_on="user_id",
                how="inner",
                output_port="OUT",
            )
        ],
    )

    outs: List[Out] = []
    for i in range(1, n + 1):
        outs += await collect_async(
            comp.process_row(
                InTagged("users", {"user": {"id": i, "name": f"N{i}"}}),
                metrics=data_ops_metrics,
            )
        )
    for i in range(2, n + 1, 2):
        outs += await collect_async(
            comp.process_row(
                InTagged("purchases", {"user_id": i, "city": f"X{i}"}),
                metrics=data_ops_metrics,
            )
        )

    outs += await collect_async(
        comp.process_row(InTagged("users", Ellipsis), metrics=data_ops_metrics)
    )
    outs += await collect_async(
        comp.process_row(InTagged("purchases", Ellipsis), metrics=data_ops_metrics)
    )

    assert [o.port for o in outs] == ["OUT"] * (n // 2)

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


@pytest.mark.asyncio
async def test_row_join_flat_to_nested_large(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    """Flat L + nested R via envelopes, inner join on id/user_id."""

    async def feed_rows(
        comp: SchemaMappingComponent, port: str, rows: List[Dict[str, Any]]
    ) -> None:
        for row in rows:
            async for _ in comp.process_row(
                InTagged(port, row), metrics=data_ops_metrics
            ):
                pass  # noqa: E701

    async def flush_port(comp: SchemaMappingComponent, port: str) -> List[Out]:
        outs: List[Out] = []
        async for out in comp.process_row(
            InTagged(port, Ellipsis), metrics=data_ops_metrics
        ):
            outs.append(out)
        return outs

    def mk_left_rows(n: int) -> List[Dict[str, Any]]:
        return [{"id": i, "name": f"user-{i}"} for i in range(n)]

    def mk_right_rows(n: int) -> List[Dict[str, Any]]:
        return [{"user_id": i, "addr": {"city": f"City-{i}"}} for i in range(n)]

    def assert_outputs(outs: List[Out], n: int) -> None:
        assert outs, "no output received from row join"
        assert all(isinstance(o, Out) for o in outs)
        assert all(o.port == "OUT" for o in outs)
        assert len(outs) == n
        for idx in (0, n // 2, n - 1):
            payload = outs[idx].payload
            assert isinstance(payload, dict)
            assert payload.get("id") == idx
            assert payload.get("name") == f"user-{idx}"
            assert "addr" in payload and isinstance(payload["addr"], dict)
            assert payload["addr"].get("city") == f"City-{idx}"

    left_schema = schema_from_fields([fd("id", "integer"), fd("name", "string")])
    right_schema = schema_from_fields(
        [fd("user_id", "integer"), fd("addr.city", "string")]
    )
    out_schema = schema_from_fields(
        [fd("id", "integer"), fd("name", "string"), fd("addr.city", "string")]
    )

    comp = make_join_component(
        name="RowJoinFlatToNested",
        description="row join flat+nested",
        in_ports=["L", "R"],
        in_port_schemas={"L": left_schema, "R": right_schema},
        out_ports=[("OUT", out_schema)],
        steps=[
            JoinStep(
                left_port="L",
                right_port="R",
                left_on="id",
                right_on="user_id",
                how="inner",
                output_port="OUT",
            )
        ],
    )
    assert comp.requires_tagged_input() is True

    n = 1000
    await feed_rows(comp, "L", mk_left_rows(n))
    await feed_rows(comp, "R", mk_right_rows(n))

    _ = await flush_port(comp, "L")
    outs = await flush_port(comp, "R")

    assert_outputs(outs, n)
