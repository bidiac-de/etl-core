import csv
from pathlib import Path
from typing import Dict, Any, List, Generator, Union

from tensorflow.python.ops.gen_io_ops import ReadFile

from src.receivers.read_file_receiver import ReadFileReceiver
from src.receivers.write_file_receiver import WriteFileReceiver

import pandas as pd
import dask.dataframe as dd

class CSVReceiver(ReadFileReceiver, WriteFileReceiver):
    def __init__(self, filepath: Path):
        self.filepath = filepath


    def read_row(self, filepath: Path = None) -> Dict[str, Any]:
        """Reads a single row from the CSV file."""
        path = filepath or self.filepath
        with open(path, newline='', encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            return next(reader, {})

    def read_bulk(self, filepath: Path = None) -> List[Dict[str, Any]]:
        """Reads all rows from the CSV file into a list of dicts."""
        path = filepath or self.filepath
        with open(path, newline='', encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            return list(reader)

    def read_bigdata(
            self,
            filepath: Path = None,
            use_dask: bool = False,
            blocksize: str = "16MB"
    ) -> Union[Generator[Dict[str, Any], None, None], pd.DataFrame, dd.DataFrame]:
        """Reads big CSV files using streaming (generator) or Dask for large datasets."""
        path = filepath or self.filepath

        if use_dask:
            return dd.read_csv(path, blocksize=blocksize)

        df = pd.read_csv(path, chunksize=10000)
        for chunk in df:
            for _, row in chunk.iterrows():
                yield row.to_dict()

    def write_row(self, row: Dict[str, Any], filepath: Path = None):
        """Writes a single row to the CSV file."""
        path = filepath or self.filepath
        file_exists = Path(path).exists()
        with open(path, mode='a', newline='', encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=row.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

    def write_bulk(self, data: List[Dict[str, Any]], filepath: Path = None):
        """Writes a list of rows to the CSV file."""
        if not data:
            return
        path = filepath or self.filepath
        with open(path, mode='w', newline='', encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

    def write_bigdata(
            self,
            data: Union[pd.DataFrame, dd.DataFrame, Generator[Dict[str, Any], None, None]],
            filepath: Path = None
    ):
        """Writes big data using pandas or dask."""
        path = filepath or self.filepath

        if isinstance(data, pd.DataFrame):
            data.to_csv(path, index=False)
        elif isinstance(data, dd.DataFrame):
            data.to_csv(str(path), single_file=True, index=False)
        else:
            first_row = next(data, None)
            if not first_row:
                return
            with open(path, mode='w', newline='', encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=first_row.keys())
                writer.writeheader()
                writer.writerow(first_row)
                for row in data:
                    writer.writerow(row)