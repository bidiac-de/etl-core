import pandas as pd
import dask.dataframe as dd
import pytest

from etl_core.components.stubcomponents import StubComponent
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType


def _row_schema() -> Schema:
    return Schema(
        fields=[
            FieldDef(name="id", data_type=DataType.INTEGER, nullable=False),
            FieldDef(name="name", data_type=DataType.STRING, nullable=False),
        ]
    )


@pytest.fixture()
def stub() -> StubComponent:
    c = StubComponent(name="stub", description="", comp_type="test")
    c.in_port_schemas = {"in": _row_schema()}
    c.out_port_schemas = {"out": _row_schema()}
    return c


# IN payload validation


def test_validate_in_payload_row_ok(stub: StubComponent) -> None:
    stub.validate_in_payload("in", {"id": 1, "name": "Nina"})


def test_validate_in_payload_row_type_error(stub: StubComponent) -> None:
    with pytest.raises(ValueError) as ex:
        stub.validate_in_payload("in", {"id": "x", "name": "Nina"})
    assert "expected DataType.INTEGER" in str(ex.value)


def test_validate_in_payload_missing_schema_raises(stub: StubComponent) -> None:
    with pytest.raises(ValueError) as ex:
        stub.validate_in_payload("not_declared", {"id": 1})
    assert "no schema configured for in port" in str(ex.value)


def test_validate_in_payload_pandas_ok(stub: StubComponent) -> None:
    stub.in_port_schemas["in"] = _row_schema()
    df = pd.DataFrame([{"id": 1, "name": "A"}])
    stub.validate_in_payload("in", df)


def test_validate_in_payload_pandas_missing_and_unknown(stub: StubComponent) -> None:
    stub.in_port_schemas["in"] = _row_schema()

    df_missing = pd.DataFrame([{"id": 1}])
    with pytest.raises(ValueError) as ex1:
        stub.validate_in_payload("in", df_missing)
    assert "missing required columns" in str(ex1.value)

    df_extra = pd.DataFrame([{"id": 1, "name": "x", "extra": 1}])
    with pytest.raises(ValueError) as ex2:
        stub.validate_in_payload("in", df_extra)
    assert "unknown columns present" in str(ex2.value)


def test_validate_in_payload_dask_ok(stub: StubComponent) -> None:
    stub.in_port_schemas["in"] = _row_schema()
    ddf = dd.from_pandas(pd.DataFrame([{"id": 1, "name": "A"}]), npartitions=1)
    stub.validate_in_payload("in", ddf)


def test_validate_in_payload_unsupported_type_raises(stub: StubComponent) -> None:
    with pytest.raises(TypeError) as ex:
        stub.validate_in_payload("in", [1, 2, 3])  # type: ignore[arg-type]
    assert "unsupported payload type list" in str(ex.value)


def test_validate_in_payload_custom_separator_affects_df(stub: StubComponent) -> None:
    StubComponent._schema_path_separator = ":"
    df_ok = pd.DataFrame([{"id": 1, "name": "A"}])
    stub.validate_in_payload("in", df_ok)


# OUT payload validation


def test_validate_out_payload_ok(stub: StubComponent) -> None:
    stub.validate_out_payload("out", {"id": 2, "name": "Max"})


def test_validate_out_payload_enum_fail(stub: StubComponent) -> None:
    stub.out_port_schemas["out"] = Schema(
        fields=[
            FieldDef(name="id", data_type=DataType.INTEGER, nullable=False),
            FieldDef(
                name="name",
                data_type=DataType.ENUM,
                nullable=False,
                enum_values=["Nina", "Lena"],
            ),
        ]
    )
    with pytest.raises(ValueError) as ex:
        stub.validate_out_payload("out", {"id": 1, "name": "Max"})
    assert "must be one of" in str(ex.value)


def test_validate_out_payload_missing_schema_raises(stub: StubComponent) -> None:
    stub.out_port_schemas.pop("out")
    with pytest.raises(ValueError) as ex:
        stub.validate_out_payload("out", {"id": 1, "name": "A"})
    assert "no schema configured for out port" in str(ex.value)
