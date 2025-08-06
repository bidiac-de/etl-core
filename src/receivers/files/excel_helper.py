from pathlib import Path
from typing import Dict, Any, List, Generator, Union
import pandas as pd
from src.receivers.files.file_helper import resolve_file_path, ensure_exists

def read_excel_row(path: Path, sheet_name: str = 0, row_index: int = 0) -> Dict[str, Any]:
    """Read a specific row from an Excel file."""
    path = resolve_file_path(path)
    ensure_exists(path)

    df = pd.read_excel(path, sheet_name=sheet_name)
    if row_index >= len(df):
        return {}
    return df.iloc[row_index].to_dict()


def read_excel_bulk(path: Path, sheet_name: str = 0) -> pd.DataFrame:
    """Read the entire Excel sheet into a Pandas DataFrame."""
    path = resolve_file_path(path)
    ensure_exists(path)
    return pd.read_excel(path, sheet_name=sheet_name)


def read_excel_bigdata(path: Path, sheet_name: str = 0) -> Generator[Dict[str, Any], None, None]:
    """Read Excel file row by row (no real chunking possible)."""
    path = resolve_file_path(path)
    ensure_exists(path)

    df = pd.read_excel(path, sheet_name=sheet_name)
    for _, row in df.iterrows():
        yield row.to_dict()


def write_excel_row(path: Path, row: Dict[str, Any], sheet_name: str = "Sheet1"):
    """Write a single row to an Excel file."""
    path = resolve_file_path(path)
    try:
        df_existing = pd.read_excel(path, sheet_name=sheet_name)
        df_new = pd.DataFrame([row])
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    except FileNotFoundError:
        df_combined = pd.DataFrame([row])

    df_combined.to_excel(path, sheet_name=sheet_name, index=False)


def write_excel_bulk(path: Path, data: List[Dict[str, Any]], sheet_name: str = "Sheet1"):
    """Write multiple rows to an Excel file."""
    path = resolve_file_path(path)
    df = pd.DataFrame(data)
    df.to_excel(path, sheet_name=sheet_name, index=False)


def write_excel_bigdata(path: Path, data: Union[pd.DataFrame, Generator[Dict[str, Any], None, None]], sheet_name: str = "Sheet1"):
    """Write big data to an Excel file."""
    path = resolve_file_path(path)

    if isinstance(data, pd.DataFrame):
        data.to_excel(path, sheet_name=sheet_name, index=False)
    elif hasattr(data, "__iter__"):
        rows = list(data)
        if rows:
            df = pd.DataFrame(rows)
            df.to_excel(path, sheet_name=sheet_name, index=False)