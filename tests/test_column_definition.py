from src.components.column_definition import ColumnDefinition

def test_column_definition():
    col = ColumnDefinition(name="age", data_type="int")
    assert col.name == "age"
    assert col.data_type == "int"