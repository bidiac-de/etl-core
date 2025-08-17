from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, field_validator


class DataType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    OBJECT = "object"
    ARRAY = "array"
    ENUM = "enum"
    PATH = "path"


class FieldDef(BaseModel):
    """
    Recursive field definition.
    - OBJECT: define children fields.
    - ARRAY: define schema of item fields.
    - ENUM: define enum_values.
    """

    name: str
    data_type: DataType
    nullable: bool = False
    enum_values: Optional[List[str]] = None
    children: Optional[List["FieldDef"]] = None
    item: Optional["FieldDef"] = None

    @field_validator("children")
    @classmethod
    def _children_only_for_objects(
        cls, v: Optional[List["FieldDef"]], values: Dict[str, Any]
    ) -> Optional[List["FieldDef"]]:
        if v and values.get("data_type") != DataType.OBJECT:
            msg = "children allowed only for OBJECT"
            raise ValueError(msg)
        return v

    @field_validator("item")
    @classmethod
    def _item_only_for_arrays(
        cls, v: Optional["FieldDef"], values: Dict[str, Any]
    ) -> Optional["FieldDef"]:
        if v and values.get("data_type") != DataType.ARRAY:
            msg = "item allowed only for ARRAY"
            raise ValueError(msg)
        return v

    @field_validator("enum_values")
    @classmethod
    def _enum_only_for_enum(
        cls, v: Optional[List[str]], values: Dict[str, Any]
    ) -> Optional[List[str]]:
        if v and values.get("data_type") != DataType.ENUM:
            msg = "enum_values allowed only for ENUM"
            raise ValueError(msg)
        return v


FieldDef.model_rebuild()  # resolve forward refs
