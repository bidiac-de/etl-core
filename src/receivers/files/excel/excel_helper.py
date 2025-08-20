from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterator, List, Optional, Union

import dask.dataframe as dd
import pandas as pd

from src.receivers.files.file_helper import ensure_exists, resolve_file_path


READABLE_EXTS = {".xlsx", ".xlsm", ".xls"}
WRITABLE_EXTS = {".xlsx", ".xlsm"}


def _ext(path: Path) -> str:
    """Return the lowercase file extension of the given path."""
    return path.suffix.lower()


def _engine_for_read(ext: str) -> str:
    """Determine the engine to use for reading Excel files based on the extension."""
    if ext in {".xlsx", ".xlsm"}:

        return "openpyxl"
    if ext == ".xls":
        try:
            import xlrd
        except Exception as exc:
            raise ValueError(
                "Reading .xls requires 'xlrd' (<2.0). Please install it "
                "or convert the file to .xlsx."
            ) from exc
        return "xlrd"
    raise ValueError(f"Unsupported excel extension for read: '{ext}'.")


def _engine_for_write(ext: str) -> str:
    """Determine the engine to use for writing Excel files based on the extension."""
    if ext in {".xlsx", ".xlsm"}:
        return "openpyxl"
    raise ValueError(
        f"Writing '{ext}' is not supported. Use one of: {sorted(WRITABLE_EXTS)}."
    )


def read_excel_rows(
    path: Path,
    sheet_name: Optional[str] = None,
) -> Iterator[Dict[str, object]]:
    """Read an Excel sheet and yield rows as dictionaries (streaming iteration)."""
    path = resolve_file_path(path)
    ensure_exists(path)
    ext = _ext(path)
    engine = _engine_for_read(ext)

    df = pd.read_excel(path, sheet_name=sheet_name or 0, engine=engine)
    for _, row in df.iterrows():
        d = row.to_dict()
        for k, v in list(d.items()):
            if pd.isna(v):
                d[k] = None
        yield d


def read_excel_bulk(path: Path, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Read an entire Excel sheet into a pandas DataFrame."""
    path = resolve_file_path(path)
    ensure_exists(path)
    ext = _ext(path)
    engine = _engine_for_read(ext)
    return pd.read_excel(path, sheet_name=sheet_name or 0, engine=engine)


def read_excel_bigdata(
    path: Path,
    sheet_name: Optional[str] = None,
    npartitions: int = 8,
) -> dd.DataFrame:
    """
    Create a Dask DataFrame from an Excel sheet (via pandas -> Dask).
    Excel does not support true chunked/big data APIs; the sheet is materialized once.
    """
    path = resolve_file_path(path)
    ensure_exists(path)
    ext = _ext(path)
    engine = _engine_for_read(ext)

    pdf = pd.read_excel(path, sheet_name=sheet_name or 0, engine=engine)
    npartitions = max(1, min(npartitions, len(pdf) or 1))
    return dd.from_pandas(pdf, npartitions=npartitions)


def _normalize_to_dataframe(
    data: Union[pd.DataFrame, List[Dict[str, object]]],
) -> pd.DataFrame:
    """Convert the given input into a pandas DataFrame if it is not one already."""
    if isinstance(data, pd.DataFrame):
        return data
    return pd.DataFrame(data or [])


def write_excel_row(
    path: Path,
    row: Dict[str, object],
    sheet_name: Optional[str] = None,
) -> None:
    """
    Append a single row to the given Excel sheet (write header if needed).

    Note:
        Writing to .xlsm produces a macro-enabled workbook, but macros will not
        be preserved if you overwrite an existing macro workbook.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ext = _ext(path)
    engine = _engine_for_write(ext)

    if path.exists():
        try:
            existing = pd.read_excel(path, sheet_name=sheet_name or 0, engine=engine)
        except Exception:
            existing = pd.DataFrame()
    else:
        existing = pd.DataFrame()

    df_row = pd.DataFrame([row])
    out = pd.concat([existing, df_row], ignore_index=True)
    out.to_excel(path, sheet_name=sheet_name or "Sheet1", index=False, engine=engine)


def write_excel_bulk(
    path: Path,
    data: Union[pd.DataFrame, List[Dict[str, object]]],
    sheet_name: Optional[str] = None,
) -> None:
    """Overwrite the target Excel sheet with the given data."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ext = _ext(path)
    engine = _engine_for_write(ext)

    df = _normalize_to_dataframe(data)
    df.to_excel(path, sheet_name=sheet_name or "Sheet1", index=False, engine=engine)


def write_excel_bigdata(
    path: Path,
    data: dd.DataFrame,
    sheet_name: Optional[str] = None,
) -> None:
    """Write a Dask DataFrame to an Excel sheet by materializing it to pandas first."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ext = _ext(path)
    engine = _engine_for_write(ext)

    pdf = data.compute()
    pdf.to_excel(path, sheet_name=sheet_name or "Sheet1", index=False, engine=engine)
