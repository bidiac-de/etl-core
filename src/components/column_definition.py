from enum import Enum

class DataType(Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    OBJECT = "object"
    PATH = "path"
    ENUM = "enum"

class ColumnDefinition:
    def __init__(self, name: str, data_type: DataType):
        self.name = name
        self.data_type = data_type