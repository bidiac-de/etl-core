from typing import List
from pydantic import BaseModel
from src.components.column_definition import ColumnDefinition


class Schema(BaseModel):
    """Defines a reusable schema consisting of multiple columns."""
    columns: List[ColumnDefinition]

    def get_column_names(self) -> List[str]:
        """Return a list of column names."""
        return [col.name for col in self.columns]