from pathlib import Path
from typing import List, Literal
from pydantic import Field, model_validator
from src.components.base_component import Component
from src.components.column_definition import ColumnDefinition
from abc import ABC


class FileComponent(Component, ABC):
    """Abstract base class for file-based components like CSV, JSON, etc."""

    filepath: Path = Field(..., description="Path to the file")

    @model_validator(mode="after")
    def validate_filepath(self) -> "FileComponent":
        if not isinstance(self.filepath, Path):
            self.filepath = Path(self.filepath)
        return self
