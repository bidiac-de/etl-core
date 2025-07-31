import csv
from pathlib import Path
from typing import Dict, Any, List, Generator, Union
import pandas as pd
import dask.dataframe as dd

from src.receivers.read_file_receiver import ReadFileReceiver
from src.receivers.write_file_receiver import WriteFileReceiver
from src.metrics.component_metrics import ComponentMetrics
from src.utils.file_utils import resolve_path, file_exists


class CSVReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for reading and writing CSV files with support for pandas and dask."""

    def read_row(self, metrics: ComponentMetrics, filepath: Path) -> Dict[str, Any]:
        """Reads a single row from the CSV file."""
        path = resolve_path(filepath)
        with open(path, newline='', encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            return next(reader, {})

    def read_bulk(self, metrics: ComponentMetrics, filepath: Path) -> List[Dict[str, Any]]:
        """Reads all rows from the CSV file into a list of dicts."""
        path = resolve_path(filepath)
        with open(path, newline='', encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            return list(reader)

    def read_bigdata(
            self,
            metrics: ComponentMetrics,
            filepath: Path,
            use_dask: bool = False,
            blocksize: str = "16MB"
    ) -> Union[Generator[Dict[str, Any], None, None], pd.DataFrame, dd.DataFrame]:
        """Reads big CSV files using streaming (generator) or Dask for large datasets."""
        path = resolve_path(filepath)

        if use_dask:
            return dd.read_csv(path, blocksize=blocksize)

        df = pd.read_csv(path, chunksize=10000)
        for chunk in df:
            for _, row in chunk.iterrows():
                yield row.to_dict()

    def write_row(self, metrics: ComponentMetrics, row: Dict[str, Any], filepath: Path):
        """Writes a single row to the CSV file."""
        path = resolve_path(filepath)
        exists = file_exists(path)
        with open(path, mode='a', newline='', encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=row.keys())
            if not exists:
                writer.writeheader()
            writer.writerow(row)

    def write_bulk(self, metrics: ComponentMetrics, data: List[Dict[str, Any]], filepath: Path):
        """Writes a list of rows to the CSV file."""
        if not data:
            return
        path = resolve_path(filepath)
        with open(path, mode='w', newline='', encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

    def write_bigdata(
            self,
            metrics: ComponentMetrics,
            data: Union[pd.DataFrame, dd.DataFrame, Generator[Dict[str, Any], None, None]],
            filepath: Path
    ):
        """Writes big data using pandas or dask."""
        path = resolve_path(filepath)

        if isinstance(data, pd.DataFrame):
            data.to_csv(path, index=False)

        elif isinstance(data, dd.DataFrame):
            data.to_csv(str(path), single_file=True, index=False)

        elif hasattr(data, "__iter__"):
            first_row = next(data, None)
            if not first_row:
                return
            with open(path, mode='w', newline='', encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=first_row.keys())
                writer.writeheader()
                writer.writerow(first_row)
                for row in data:
                    writer.writerow(row)