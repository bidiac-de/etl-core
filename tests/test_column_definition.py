from etl_core.components.column_definition import ColumnDefinition, DataType


def test_column_definition():
    col = ColumnDefinition(name="age", data_type=DataType.INTEGER)
    assert col.name == "age"
    assert col.data_type == DataType.INTEGER
