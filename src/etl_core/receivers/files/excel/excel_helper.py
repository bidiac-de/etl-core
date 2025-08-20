from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Union, Any

import dask.dataframe as dd
import pandas as pd

from src.etl_core.receivers.files.file_helper import ensure_exists, resolve_file_path

READABLE_EXTS = {".xlsx", ".xlsm", ".xls"}
WRITABLE_EXTS = {".xlsx", ".xlsm"}

SHEET_DEFAULT = "Sheet1"


def _ext(path: Path) -> str:
    return path.suffix.lower()


def _engine_for_read(ext: str) -> str:
    """Determine the engine to use for reading Excel files based on the extension."""
    if ext in {".xlsx", ".xlsm"}:
        return "openpyxl"
    if ext == ".xls":
        return "xlrd"
    raise ValueError(f"Unsupported excel extension for read: '{ext}'.")


def _engine_for_write(ext: str) -> str:
    if ext in WRITABLE_EXTS:
        return "openpyxl"
    raise ValueError(
        f"Writing '{ext}' is not supported. Use one of: {sorted(WRITABLE_EXTS)}."
    )


def _prepare_read(path: Path) -> tuple[Path, str]:
    path = resolve_file_path(path)
    ensure_exists(path)
    return path, _engine_for_read(_ext(path))


def _prepare_write(path: Path) -> tuple[Path, str]:
    # Resolve into the same root the reads use (keeps behavior consistent)
    path = resolve_file_path(Path(path))
    path.parent.mkdir(parents=True, exist_ok=True)
    return path, _engine_for_write(_ext(path))


def _df_replace_nans_inplace(df: pd.DataFrame) -> pd.DataFrame:
    # Vectorized NaN->None
    return df.where(pd.notna(df), None)


def read_excel_rows(path: Path, sheet_name: Optional[str] = None) -> Iterator[Dict[str, object]]:
    path, engine = _prepare_read(path)
    df = pd.read_excel(path, sheet_name=sheet_name or 0, engine=engine)
    df = _df_replace_nans_inplace(df)
    # Iteration using records
    for rec in df.to_dict(orient="records"):
        yield rec


def read_excel_bulk(path: Path, sheet_name: Optional[str] = None) -> pd.DataFrame:
    path, engine = _prepare_read(path)
    df = pd.read_excel(path, sheet_name=sheet_name or 0, engine=engine)
    return _df_replace_nans_inplace(df)


def read_excel_bigdata(
    path: Path,
    sheet_name: Optional[str] = None,
    npartitions: int = 8,
) -> dd.DataFrame:
    path, engine = _prepare_read(path)
    pdf = pd.read_excel(path, sheet_name=sheet_name or 0, engine=engine)
    pdf = _df_replace_nans_inplace(pdf)
    npartitions = max(1, min(npartitions, max(len(pdf), 1)))
    return dd.from_pandas(pdf, npartitions=npartitions)


def _normalize_to_dataframe(data: Union[pd.DataFrame, Sequence[Dict[str, object]]]) -> pd.DataFrame:
    return data if isinstance(data, pd.DataFrame) else pd.DataFrame(list(data) if data else [])


def write_excel_row(path: Path, row: Dict[str, object], sheet_name: Optional[str] = None) -> None:
    """
    Append a single row efficiently. Avoids re-reading the entire file by using openpyxl.
    Falls back to pandas path if anything unexpected happens (e.g., wrong engine).
    """
    path, engine = _prepare_write(path)

    if engine == "openpyxl":
        try:
            from openpyxl import load_workbook
        except Exception:  # pragma: no cover - hard dep edge case
            # Fallback to pandas-based concat if openpyxl is not available
            _write_excel_row_pandas_fallback(path, row, sheet_name or SHEET_DEFAULT, engine)
            return

        if path.exists():
            wb = load_workbook(path)
            ws = wb[sheet_name or SHEET_DEFAULT] if sheet_name or SHEET_DEFAULT in wb.sheetnames else wb.active
            if ws.max_row == 1 and all(cell.value is None for cell in ws[1]):
                # Empty sheet: write header + row
                keys = list(row.keys())
                ws.append(keys)
                ws.append([row.get(k) for k in keys])
            else:
                # Header already exists; align values by header order
                header = [cell.value for cell in ws[1]]
                ws.append([row.get(k) for k in header])
            wb.save(path)
            return

        _write_excel_row_pandas_fallback(path, row, sheet_name or SHEET_DEFAULT, engine)
        return

    # Non-openpyxl engines -> pandas fallback
    _write_excel_row_pandas_fallback(path, row, sheet_name or SHEET_DEFAULT, engine)


def _write_excel_row_pandas_fallback(path: Path, row: Dict[str, Any], sheet_name: str, engine: str) -> None:
    if path.exists():
        try:
            existing = pd.read_excel(path, sheet_name=sheet_name, engine=engine)
        except Exception:
            existing = pd.DataFrame()
    else:
        existing = pd.DataFrame()
    out = pd.concat([existing, pd.DataFrame([row])], ignore_index=True)
    out.to_excel(path, sheet_name=sheet_name, index=False, engine=engine)


def write_excel_bulk(
    path: Path,
    data: Union[pd.DataFrame, List[Dict[str, object]]],
    sheet_name: Optional[str] = None,
) -> None:
    path, engine = _prepare_write(path)
    df = _normalize_to_dataframe(data)
    df.to_excel(path, sheet_name=sheet_name or SHEET_DEFAULT, index=False, engine=engine)


def write_excel_bigdata(
    path: Path,
    data: dd.DataFrame,
    sheet_name: Optional[str] = None,
) -> None:
    path, engine = _prepare_write(path)
    pdf = data.compute()
    pdf.to_excel(path, sheet_name=sheet_name or SHEET_DEFAULT, index=False, engine=engine)
