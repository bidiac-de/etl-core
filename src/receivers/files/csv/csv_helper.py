import csv
from pathlib import Path
from typing import Dict, Any, List, Generator, Iterable, Union
import itertools
import pandas as pd
import dask.dataframe as dd

from src.receivers.files.file_helper import resolve_file_path, ensure_exists, open_file


def read_csv_row(path: Path, separator: str = ",") -> Generator[Dict[str, Any], None, None]:

    """Yield CSV rows as dictionaries (synchronous generator)."""
    path = resolve_file_path(path)
    ensure_exists(path)
    with open_file(path, "r", newline="") as f:
        reader = csv.DictReader(f, delimiter=separator)

    for row in reader:
            yield row


def read_csv_bulk(path: Path) -> pd.DataFrame:
    """Read the entire CSV into a Pandas DataFrame."""
    path = resolve_file_path(path)
    ensure_exists(path)
    return pd.read_csv(path, dtype=str)


def read_csv_bigdata(path: Path, blocksize: str = "16MB") -> dd.DataFrame:
    """Read large CSV files efficiently using Dask."""
    path = resolve_file_path(path)
    ensure_exists(path)
    return dd.read_csv(path, blocksize=blocksize, dtype=str)


def write_csv_row(path: Path, row: Dict[str, Any]):
    """Write a single row to a CSV file, adding header if needed."""
    path = resolve_file_path(path)
    file_exists_flag = path.exists() and path.stat().st_size > 0

    with open_file(path, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=row.keys())
        if not file_exists_flag:
            writer.writeheader()
        writer.writerow(row)


def write_csv_bulk(path: Path, data: List[Dict[str, Any]]):
    """Write multiple rows to a CSV file."""
    path = resolve_file_path(path)
    if not data:
        with open_file(path, "w", newline=""):
            return

    with open_file(path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)


def write_csv_bigdata(path: Path, data: Union[dd.DataFrame, Iterable[Dict[str, Any]]]):
    """Write large datasets to the CSV file using Dask or an iterable of rows."""
    path = resolve_file_path(path)

    if isinstance(data, dd.DataFrame):
        delayed = data.to_csv(str(path), single_file=True, index=False)
        try:
            delayed.compute()
        except AttributeError:
            pass
        return

    it = iter(data)
    first_row = next(it, None)
    if first_row is None:
        with open_file(path, "w", newline=""):
            return

    with open_file(path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=list(first_row.keys()))
        writer.writeheader()
        for row in itertools.chain([first_row], it):
            writer.writerow(row)