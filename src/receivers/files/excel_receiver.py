from pathlib import Path
from typing import Dict, Any, List, Generator, Union
import pandas as pd

from src.receivers.read_file_receiver import ReadFileReceiver
from src.receivers.write_file_receiver import WriteFileReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.excel_helper import (
    read_excel_row,
    read_excel_bulk,
    read_excel_bigdata,
    write_excel_row,
    write_excel_bulk,
    write_excel_bigdata
)


class ExcelReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for Excel files using pandas."""

    def __init__(self, filepath: Path):
        self.filepath: Path = filepath

    def read_row(self, metrics: ComponentMetrics, sheet_name: str = 0, row_index: int = 0) -> Dict[str, Any]:
        return read_excel_row(self.filepath, sheet_name, row_index)

    def read_bulk(self, metrics: ComponentMetrics, sheet_name: str = 0) -> pd.DataFrame:
        return read_excel_bulk(self.filepath, sheet_name)

    def read_bigdata(self, metrics: ComponentMetrics, sheet_name: str = 0) -> Generator[Dict[str, Any], None, None]:
        return read_excel_bigdata(self.filepath, sheet_name)

    def write_row(self, metrics: ComponentMetrics, row: Dict[str, Any], sheet_name: str = "Sheet1"):
        write_excel_row(self.filepath, row, sheet_name)

    def write_bulk(self, metrics: ComponentMetrics, data: List[Dict[str, Any]], sheet_name: str = "Sheet1"):
        write_excel_bulk(self.filepath, data, sheet_name)

    def write_bigdata(self, metrics: ComponentMetrics, data: Union[pd.DataFrame, Generator[Dict[str, Any], None, None]], sheet_name: str = "Sheet1"):
        write_excel_bigdata(self.filepath, data, sheet_name)