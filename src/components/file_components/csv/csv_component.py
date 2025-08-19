from abc import ABC
from enum import Enum

from pydantic import Field, ConfigDict
from src.components.file_components.file_component import FileComponent


class Delimiter(str, Enum):
    """Enum for possible CSV delimiters."""

    COMMA = ","
    SEMICOLON = ";"
    TAB = "\t"


class CSV(FileComponent, ABC):
    """Abstract base class for CSV file components."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    separator: Delimiter = Field(
        default=Delimiter.COMMA, description="CSV field separator"
    )
