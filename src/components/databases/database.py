from abc import ABC
from typing import List
from src.components.base_component import Component
from src.components.column_definition import ColumnDefinition
from src.context.context import Context
from pydantic import Field


class DatabaseComponent(Component, ABC):
    context: Context = Field(default_factory=lambda: Context())
    schema_definition: List[ColumnDefinition] = Field(default_factory=list)
    credentials_id: str = Field(
        default="", description="ID of the credentials in ContextRegistry"
    )
