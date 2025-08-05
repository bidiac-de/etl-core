from pathlib import Path
from typing import Dict, Any, List, Generator, Union, Optional
import pandas as pd
import dask.dataframe as dd

from src.receivers.read_file_receiver import ReadFileReceiver
from src.receivers.write_file_receiver import WriteFileReceiver
from src.metrics.component_metrics import ComponentMetrics
from src.receivers.files.file_helper import resolve_file_path, ensure_exists, file_exists
from src.receivers.files.csv_helper import (
    read_csv_row,
    read_csv_bulk,
    read_csv_bigdata,
    write_csv_row,
    write_csv_bulk,
    write_csv_bigdata
)


class CSVReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for CSV files with support for Pandas and Dask."""

    def __init__(self, filepath: Path):
        """Initialize the CSVReceiver with a fixed filepath."""
        self.filepath: Path = resolve_file_path(filepath)


    def read_row(self, metrics: ComponentMetrics, line_number: int = 0) -> Dict[str, Any]:
        """
        Read a single row from the CSV file by line number.
        Default: first row.
        """
        return read_csv_row(self.filepath, line_number)

    def read_bulk(self, metrics: ComponentMetrics) -> pd.DataFrame:
        """Read the entire CSV file into a Pandas DataFrame."""
        return read_csv_bulk(self.filepath)

    def read_bigdata(
            self,
            metrics: ComponentMetrics,
            use_dask: bool = False,
            blocksize: str = "16MB"
    ) -> Union[Generator[Dict[str, Any], None, None], pd.DataFrame, dd.DataFrame]:
        """Read large CSV files efficiently using Pandas (streaming) or Dask."""
        return read_csv_bigdata(self.filepath, use_dask, blocksize)

    def write_row(self, metrics: ComponentMetrics, row: Dict[str, Any]):
        """Write a single row to the CSV file."""
        write_csv_row(self.filepath, row)

    def write_bulk(self, metrics: ComponentMetrics, data: List[Dict[str, Any]]):
        """Write multiple rows to the CSV file."""
        if data:
            write_csv_bulk(self.filepath, data)

    def write_bigdata(
            self,
            metrics: ComponentMetrics,
            data: Union[pd.DataFrame, dd.DataFrame, Generator[Dict[str, Any], None, None]],
    ):
        """Write large datasets to the CSV file using Pandas, Dask, or a generator."""
        write_csv_bigdata(self.filepath, data)