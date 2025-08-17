from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Union

import pandas as pd
import dask.dataframe as dd

try:
    import openpyxl
except Exception:
    openpyxl = None

from src.receivers.files.file_helper import (
    ensure_exists,
    resolve_file_path,
)

SUPPORTED_EXTS = {".xlsx", ".xlsm"}


def _validate_ext(path: Path) -> None:
    """Allow only .xlsx and .xlsm; raise for .xls and others."""
    ext = path.suffix.lower()
    if ext not in SUPPORTED_EXTS:
        raise ValueError(
            f"Unsupported Excel extension '{ext}'. "
            "Supported: .xlsx, .xlsm"
        )


def _engine() -> str:
    return "openpyxl"


def read_excel_rows(
        path: Path,
        sheet_name: Optional[str] = None,
) -> Iterator[Dict[str, object]]:
    """Yield rows from an Excel sheet as dictionaries."""
    path = resolve_file_path(path)
    ensure_exists(path)
    _validate_ext(path)
    df = pd.read_excel(path, sheet_name=sheet_name or 0, engine=_engine())
    for _, row in df.iterrows():
        yield {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}


def read_excel_bulk(path: Path, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Read a sheet into a pandas DataFrame."""
    path = resolve_file_path(path)
    ensure_exists(path)
    _validate_ext(path)
    return pd.read_excel(path, sheet_name=sheet_name or 0, engine=_engine())


def read_excel_bigdata(
        path: Path, sheet_name: Optional[str] = None, npartitions: int = 8
) -> dd.DataFrame:
    """Create a Dask DataFrame from the Excel sheet (via pandas)."""
    path = resolve_file_path(path)
    ensure_exists(path)
    _validate_ext(path)
    pdf = pd.read_excel(path, sheet_name=sheet_name or 0, engine=_engine())
    npartitions = max(1, min(npartitions, len(pdf) or 1))
    return dd.from_pandas(pdf, npartitions=npartitions)


def _normalize_to_dataframe(
        data: Union[pd.DataFrame, List[Dict[str, object]]]
) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data
    return pd.DataFrame(data or [])


def write_excel_row(
        path: Path, row: Dict[str, object], sheet_name: Optional[str] = None
) -> None:
    """Append a single row to the given Excel sheet (header created if needed).

    Note: Writing to .xlsm produces a macro-enabled workbook, but macros will not
    be preserved if you overwrite an existing macro workbook.
    """
    _validate_ext(Path(path))
    if Path(path).exists():
        try:
            df_existing = pd.read_excel(
                path, sheet_name=sheet_name or 0, engine=_engine()
            )
        except Exception:
            df_existing = None
    else:
        df_existing = None

    df_row = pd.DataFrame([row])
    out = pd.concat([df_existing, df_row], ignore_index=True) if (
            df_existing is not None and not df_existing.empty
    ) else df_row

    out.to_excel(
        path, sheet_name=sheet_name or "Sheet1", index=False, engine=_engine()
    )


def write_excel_bulk(
        path: Path,
        data: Union[pd.DataFrame, List[Dict[str, object]]],
        sheet_name: Optional[str] = None,
) -> None:
    """Overwrite the target sheet with the given data."""
    _validate_ext(Path(path))
    df = _normalize_to_dataframe(data)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(
        path, sheet_name=sheet_name or "Sheet1", index=False, engine=_engine()
    )


def write_excel_bigdata(
        path: Path, data: dd.DataFrame, sheet_name: Optional[str] = None
) -> None:
    """Write a Dask DataFrame by materializing it to pandas first."""
    _validate_ext(Path(path))
    pdf = data.compute()
    pdf.to_excel(
        path, sheet_name=sheet_name or "Sheet1", index=False, engine=_engine()
    )