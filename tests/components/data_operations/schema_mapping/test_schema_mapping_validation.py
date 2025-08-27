from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from etl_core.components.data_operations.schema_mapping.mapping_rule import (
    FieldMapping,
)
from etl_core.components.data_operations.schema_mapping.schema_mapping_component import (  # noqa: E501
    SchemaMappingComponent,
)
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef


def _metrics() -> DataOperationsMetrics:
    return DataOperationsMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
        lines_dismissed=0,
    )


def _in_schema_user() -> Schema:
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


def _out_schema_a() -> Schema:
    return Schema(
        fields=[
            FieldDef(name="uid", data_type="integer"),
            FieldDef(name="uname", data_type="string"),
        ]
    )


def test_collision_detection_raises() -> None:
    with pytest.raises(ValueError, match="duplicate mapping"):
        SchemaMappingComponent(
            name="Collide",
            description="",
            comp_type="schema_mapping",
            extra_input_ports=["in"],
            in_port_schemas={"in": _in_schema_user()},
            extra_output_ports=["A"],
            out_port_schemas={"A": _out_schema_a()},
            rules=[
                FieldMapping(
                    src_port="in",
                    src_path="user.id",
                    dst_port="A",
                    dst_path="uid",
                ),
                FieldMapping(
                    src_port="in",
                    src_path="user.name",
                    dst_port="A",
                    dst_path="uid",  # same destination -> collision
                ),
            ],
        )


def test_unknown_source_path_raises() -> None:
    with pytest.raises(ValueError, match="unknown source path"):
        SchemaMappingComponent(
            name="BadSrc",
            description="",
            comp_type="schema_mapping",
            extra_input_ports=["in"],
            in_port_schemas={"in": _in_schema_user()},
            extra_output_ports=["A"],
            out_port_schemas={"A": _out_schema_a()},
            rules=[
                FieldMapping(
                    src_port="in",
                    src_path="user.nope",  # not in schema
                    dst_port="A",
                    dst_path="uid",
                )
            ],
        )


def test_unknown_destination_path_raises() -> None:
    with pytest.raises(ValueError, match="unknown destination path"):
        SchemaMappingComponent(
            name="BadDst",
            description="",
            comp_type="schema_mapping",
            extra_input_ports=["in"],
            in_port_schemas={"in": _in_schema_user()},
            extra_output_ports=["A"],
            out_port_schemas={"A": _out_schema_a()},
            rules=[
                FieldMapping(
                    src_port="in",
                    src_path="user.id",
                    dst_port="A",
                    dst_path="does.not.exist",  # not in schema
                )
            ],
        )


def test_traversal_into_non_object_is_reported_as_unknown_destination() -> None:
    with pytest.raises(ValueError, match="unknown destination path"):
        SchemaMappingComponent(
            name="Traverse",
            description="",
            comp_type="schema_mapping",
            extra_input_ports=["in"],
            in_port_schemas={"in": _in_schema_user()},
            extra_output_ports=["A"],
            out_port_schemas={"A": _out_schema_a()},
            rules=[
                FieldMapping(
                    src_port="in",
                    src_path="user.id",
                    dst_port="A",
                    dst_path="uid.child",
                )
            ],
        )


def test_leaf_type_mismatch_raises() -> None:
    with pytest.raises(ValueError, match="type mismatch"):
        SchemaMappingComponent(
            name="TypeMismatch",
            description="",
            comp_type="schema_mapping",
            extra_input_ports=["in"],
            in_port_schemas={"in": _in_schema_user()},
            extra_output_ports=["A"],
            out_port_schemas={
                "A": Schema(fields=[FieldDef(name="uid", data_type="integer")])
            },
            rules=[
                FieldMapping(
                    src_port="in",
                    src_path="user.name",
                    dst_port="A",
                    dst_path="uid",
                )
            ],
        )
