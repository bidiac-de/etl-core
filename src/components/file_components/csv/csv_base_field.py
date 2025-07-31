from pathlib import Path
from typing import List
from pydantic import Field
from src.components.file_components.csv.csv_component import CSV, Delimiter
from src.components.column_definition import ColumnDefinition
from src.metrics.component_metrics import ComponentMetrics
from src.receivers.files.csv_receiver import CSVReceiver


class CSVBaseFields(CSV):
    """Contains the common fields for CSV components."""
    filepath: Path = Field(..., description="Path to the CSV file")
    separator: Delimiter = Field(default=Delimiter.COMMA, description="CSV field separator")
    schema_definition: List[ColumnDefinition] = Field(..., description="Schema definition for CSV columns")

    metrics: ComponentMetrics = None
    receiver: CSVReceiver = None
