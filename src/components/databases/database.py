from abc import ABC
from typing import List
from src.components.base_component import Component
from src.components.column_definition import ColumnDefinition
from src.components.databases.connection_handler import ConnectionHandler
from src.context.context import Context
from pydantic import Field


class DatabaseComponent(Component, ABC):
    context: Context = Field(default_factory=lambda: Context())
    connection_handler: ConnectionHandler = Field(
        default_factory=lambda: ConnectionHandler()
    )
    schema_definition: List[ColumnDefinition] = Field(default_factory=list)
