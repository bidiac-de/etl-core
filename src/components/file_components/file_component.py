from pathlib import Path
from typing import List, Any, Optional
from pydantic import Field, model_validator
from src.components.base_components import Component
from src.components.column_definition import ColumnDefinition
from abc import ABC


class FileComponent(Component, ABC):
    """Abstract base class for file-based components like CSV, JSON, etc."""

    filepath: Path = Field(..., description="Path to the file")
    schema_definition: List[ColumnDefinition] = Field(..., description="Schema definition of the file")