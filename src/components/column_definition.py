from enum import Enum
from pydantic import BaseModel


class DataType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    OBJECT = "object"
    PATH = "path"
    ENUM = "enum"


class ColumnDefinition(BaseModel):
    name: str
    data_type: DataType