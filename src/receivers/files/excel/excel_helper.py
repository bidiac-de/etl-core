from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import pandas as pd

from src.receivers.files.file_helper import ensure_exists, resolve_file_path


class ExcelHelper:
    """
    Utilities to read/write Excel files with correct engine selection.

    Supported extensions:
      - .xlsx, .xlsm -> openpyxl
      - .xls         -> xlrd (read), xlwt (write)
    """


    @staticmethod
    def _engine_for_read(path: Path) -> str:
        ext = path.suffix.lower()
        if ext in (".xlsx", ".xlsm"):
            return "openpyxl"
        if ext == ".xls":
            return "xlrd"
        raise ValueError(f"Unsupported Excel file extension for read: {ext}")

    @staticmethod
    def _engine_for_write(path: Path) -> str:
        ext = path.suffix.lower()
        if ext in (".xlsx", ".xlsm"):
            return "openpyxl"
        if ext == ".xls":
            return "xlwt"
        raise ValueError(f"Unsupported Excel file extension for write: {ext}")


    @staticmethod
    def read_excel(
            filepath: Path,
            *,
            sheet_name: Optional[str | int] = 0,
            **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Read an Excel file into a pandas DataFrame after resolving and checking path.
        """
        p = resolve_file_path(filepath)
        ensure_exists(p)
        engine = ExcelHelper._engine_for_read(p)
        return pd.read_excel(p, engine=engine, sheet_name=sheet_name, **kwargs)

    @staticmethod
    def write_excel(
            df: pd.DataFrame,
            filepath: Path,
            *,
            sheet_name: Optional[str] = "Sheet1",
            append: bool = False,
            if_sheet_exists: str = "overlay",
            index: bool = False,
            **kwargs: Any,
    ) -> None:
        """
        Write a DataFrame to an Excel file with the correct engine.

        Notes:
        - Append is supported only for .xlsx/.xlsm (openpyxl).
        - For .xls, append falls back to overwrite (xlwt limitation).
        """
        p = resolve_file_path(filepath)
        engine = ExcelHelper._engine_for_write(p)
        mode = "a" if append and engine == "openpyxl" else "w"

        if engine == "openpyxl" and mode == "a":
            with pd.ExcelWriter(
                    p,
                    engine=engine,
                    mode=mode,
                    if_sheet_exists=if_sheet_exists,
                    **kwargs,
            ) as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=index)
        else:
            with pd.ExcelWriter(p, engine=engine, mode=mode, **kwargs) as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=index)