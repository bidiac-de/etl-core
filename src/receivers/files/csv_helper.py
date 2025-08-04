import csv
from pathlib import Path
from typing import Dict, Any, List, Generator, Union
import pandas as pd
import dask.dataframe as dd

from src.receivers.files.file_helper import resolve_file_path, ensure_exists, open_file, file_exists



def read_csv_row(path: Path, line_number: int = 0) -> Dict[str, Any]:
    """Read a specific row from a CSV file by line number."""
    path = resolve_file_path(path)
    ensure_exists(path)

    with open_file(path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader):
            if i == line_number:
                return row
    return {}


def read_csv_bulk(path: Path) -> pd.DataFrame:
    """Read the entire CSV into a Pandas DataFrame."""
    path = resolve_file_path(path)
    ensure_exists(path)
    return pd.read_csv(path)


def read_csv_bigdata(path: Path, use_dask: bool = False, blocksize: str = "16MB") \
        -> Union[Generator[Dict[str, Any], None, None], pd.DataFrame, dd.DataFrame]:
    """Read large CSV files using Pandas (streaming) or Dask."""
    path = resolve_file_path(path)
    ensure_exists(path)

    if use_dask:
        return dd.read_csv(path, blocksize=blocksize)

    df = pd.read_csv(path, chunksize=10000)
    for chunk in df:
        for _, row in chunk.iterrows():
            yield row.to_dict()
def write_csv_row(path: Path, row: Dict[str, Any], exists: bool):
    """Write a single row to a CSV file."""
    path = resolve_file_path(path)
    with open_file(path, "a") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=row.keys())
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def write_csv_bulk(path: Path, data: List[Dict[str, Any]]):
    """Write multiple rows to a CSV file."""
    path = resolve_file_path(path)
    with open_file(path, "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


def write_csv_bigdata(path: Path, data: Union[pd.DataFrame, dd.DataFrame, Generator[Dict[str, Any], None, None]]):
    """Write large datasets to a CSV file using Pandas, Dask, or a generator."""
    path = resolve_file_path(path)

    if isinstance(data, pd.DataFrame):
        data.to_csv(path, index=False)

    elif isinstance(data, dd.DataFrame):
        data.to_csv(str(path), single_file=True, index=False)

    elif hasattr(data, "__iter__"):
        first_row = next(data, None)
        if not first_row:
            return
        with open_file(path, "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=first_row.keys())
            writer.writeheader()
            writer.writerow(first_row)
            for row in data:
                writer.writerow(row)