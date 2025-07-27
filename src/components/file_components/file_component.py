from pathlib import Path
from typing import List, Dict, Any
from src.components.base_components import Component
from src.components.column_definition import ColumnDefinition


class FileComponent(Component):
    """Abstract base class for file-based components like CSV, JSON, etc."""

    def __init__(
        self,
        id: int,
        name: str,
        description: str,
        componentManager,
        filepath: Path,
        schema_definition: List[ColumnDefinition],
    ):
        super().__init__(
            id=id,
            name=name,
            description=description,
            type="file",
            componentManager=componentManager,
        )
        self.filepath = filepath
        self.schema_definition = schema_definition
