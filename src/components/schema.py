from __future__ import annotations
from typing import List
from pydantic import BaseModel, Field, model_validator

from src.components.column_definition import FieldDef


class Schema(BaseModel):
    """
    Schema of the input or output data for a component.
    Contains a list of field definitions, which can be nested.
    """

    fields: List[FieldDef] = Field(default_factory=list)

    @model_validator(mode="after")
    def _reject_empty(self) -> "Schema":
        # Keep it simple and explicit: empty schemas are invalid.
        if not self.fields:
            raise ValueError("Schema.fields must contain at least one FieldDef.")
        return self
