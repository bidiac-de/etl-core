import csv
from pathlib import Path
from typing import Dict, Any, Generator
import pandas as pd
import dask.dataframe as dd

from etl_core.receivers.files.file_helper import (
    resolve_file_path,
    ensure_exists,
    open_file,
)


def read_csv_row(path: Path, separator: str) -> Generator[Dict[str, Any], None, None]:
    """
    Yield CSV rows as dicts, read sequentially from file
    """
    path = resolve_file_path(path)
    ensure_exists(path)
    with open_file(path, "r", newline="") as f:
        reader = csv.DictReader(f, delimiter=separator)
        for row in reader:
            yield row


def read_csv_bulk(path: Path, separator: str) -> pd.DataFrame:
    """
    Read entire CSV into a pandas DataFrame.
    """
    path = resolve_file_path(path)
    ensure_exists(path)
    return pd.read_csv(path, dtype=str, sep=separator)


def read_csv_bigdata(
    path: Path, separator: str, blocksize: str = "16MB"
) -> dd.DataFrame:
    """
    Read large CSV as a Dask DataFrame in chunks.
    """
    path = resolve_file_path(path)
    ensure_exists(path)
    return dd.read_csv(path, blocksize=blocksize, dtype=str, sep=separator)


def write_csv_row(path: Path, row: Dict[str, Any], separator: str):
    """
    Append one row to CSV, writing header if needed.
    """
    path = resolve_file_path(path)
    file_exists_flag = path.exists() and path.stat().st_size > 0
    with open_file(path, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=row.keys(), delimiter=separator)
        if not file_exists_flag:
            writer.writeheader()
        writer.writerow(row)


def write_csv_bulk(path: Path, data: pd.DataFrame, separator: str):
    """
    Write multiple rows to CSV.
    """
    path = resolve_file_path(path)

    if data.empty:
        # Create empty file
        with open_file(path, "w", newline=""):
            return

    # use pandas directly
    with open_file(path, "w", newline="") as f:
        data.to_csv(f, index=False, sep=separator)


def write_csv_bigdata(path: Path, data: dd.DataFrame, separator: str):
    """
    Write large datasets to CSV using Dask.
    """

    path = Path(path)
    result = data.to_csv(str(path), single_file=True, index=False, sep=separator)

    if result is not None:
        try:
            if isinstance(result, (list, tuple)):
                dd.compute(*result)
            else:
                result.compute()
        except AttributeError:
            pass
