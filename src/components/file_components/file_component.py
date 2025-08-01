from pathlib import Path
from typing import List
from pydantic import Field, model_validator
from src.components.base_components import Component
from src.components.column_definition import ColumnDefinition
from abc import ABC


class FileComponent(Component, ABC):
    """Abstract base class for file-based components like CSV, JSON, etc."""

    filepath: Path = Field(..., description="Path to the file")
    schema_definition: List[ColumnDefinition] = Field(..., description="Schema definition of the file")

    @model_validator(mode="after")
    def validate_filepath(self) -> "FileComponent":
        """Ensure that filepath is a valid path."""
        if not self.filepath or not isinstance(self.filepath, Path):
            raise ValueError("Invalid filepath: must be a pathlib.Path object")
        if not self.filepath.exists():
            raise ValueError(f"Filepath does not exist: {self.filepath}")
        return self