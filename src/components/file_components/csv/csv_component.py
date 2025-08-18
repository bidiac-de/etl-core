from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import Field, ConfigDict
from src.components.file_components.file_component import FileComponent
from src.metrics.component_metrics.component_metrics import ComponentMetrics


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
