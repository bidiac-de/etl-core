from pathlib import Path
from typing import List, Dict, Any, Optional
from pydantic import Field
from src.components.base_components import Component
from src.components.column_definition import ColumnDefinition


class FileComponent(Component):
    """Abstract base class for file-based components like CSV, JSON, etc."""

    comp_type: str = Field(default="file", description="Type of the component")
    filepath: Path = Field(..., description="Path to the file")
    schema_definition: List[ColumnDefinition] = Field(..., description="Schema definition of the file")
    componentManager: Optional[Any] = Field(default=None, exclude=True)