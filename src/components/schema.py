from typing import List
from pydantic import BaseModel, ConfigDict
from src.components.column_definition import ColumnDefinition


class Schema(BaseModel):
    """Defines a reusable schema consisting of multiple columns."""

    columns: List[ColumnDefinition]

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    def get_column_names(self) -> List[str]:
        """Return a list of column names."""
        return [col.name for col in self.columns]