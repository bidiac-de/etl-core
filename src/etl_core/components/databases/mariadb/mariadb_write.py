from typing import Any, Dict, AsyncIterator, List, Union
import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from .mariadb import MariaDBComponent
from src.etl_core.components.component_registry import register_component
from src.etl_core.metrics.component_metrics.component_metrics import ComponentMetrics


@register_component("write_mariadb")
class MariaDBWrite(MariaDBComponent):
    """MariaDB writer supporting row, bulk, and bigdata modes."""

    # Write-specific fields
    table: str = Field(..., description="Table to write to")
    strategy_type: str = Field(default="bulk", description="Execution strategy type")

    # Optional fields for write operations
    batch_size: int = Field(default=1000, description="Batch size for bulk operations")
    on_duplicate_key_update: List[str] = Field(
        default_factory=list, description="Columns to update on duplicate key"
    )

    @model_validator(mode="after")
    def _build_objects(self):
        """Build objects after validation."""
        # Call parent _build_objects first
        super()._build_objects()

        # Setup connection with credentials
        self._setup_connection()

        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        """Write a single row and yield it."""
        result = await self._receiver.write_row(
            db=None,
            entity_name=self.table,
            row=row,
            metrics=metrics,
            table=self.table
        )
        yield row

    async def process_bulk(
        self, data: Union[List[Dict[str, Any]], pd.DataFrame], metrics: ComponentMetrics
    ) -> pd.DataFrame:
        """Write full dataset and yield it as DataFrame."""
        # Convert list to DataFrame if needed
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data

        result = await self._receiver.write_bulk(
            db=None,
            entity_name=self.table,
            frame=df,
            metrics=metrics,
            table=self.table
        )
        return result

    async def process_bigdata(
        self, chunk_iterable: dd.DataFrame, metrics: ComponentMetrics
    ) -> dd.DataFrame:
        """Write Dask DataFrame and yield it."""
        result = await self._receiver.write_bigdata(
            db=None,
            entity_name=self.table,
            frame=chunk_iterable,
            metrics=metrics,
            table=self.table
        )
        return result

    def _build_insert_query(self, columns: List[str]) -> str:
        """Build INSERT query with optional ON DUPLICATE KEY UPDATE."""
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])

        query = f"INSERT INTO {self.table} ({columns_str}) VALUES ({placeholders})"

        if self.on_duplicate_key_update:
            update_clause = ", ".join(
                [f"{col} = VALUES({col})" for col in self.on_duplicate_key_update]
            )
            query += f" ON DUPLICATE KEY UPDATE {update_clause}"

        return query
