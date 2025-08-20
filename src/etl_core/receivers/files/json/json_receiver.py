import asyncio
import contextlib
import os
import tempfile
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, TypeVar

import dask.dataframe as dd
import pandas as pd

from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.file_helper import (
    FileReceiverError,
    ensure_exists,
)
from etl_core.receivers.files.read_file_receiver import ReadFileReceiver
from etl_core.receivers.files.write_file_receiver import WriteFileReceiver
from etl_core.receivers.files.json.json_helper import (
    append_ndjson_record,
    dump_json_records,
    is_ndjson_path,
    load_json_records,
    read_json_row,
    iter_ndjson_lenient,
)

_SENTINEL: Any = object()
T = TypeVar("T")


def _next_or_sentinel(it: Any) -> Any:
    try:
        return next(it)
    except StopIteration:
        return _SENTINEL


def _atomic_overwrite(path: Path, writer: Callable[[Path], None]) -> None:
    """
    Write to a temp file and atomically replace the target.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent)) as tmp:
        tmp_path = Path(tmp.name)
    try:
        writer(tmp_path)
        os.replace(tmp_path, path)
    except Exception:
        with contextlib.suppress(Exception):
            tmp_path.unlink(missing_ok=True)  # type: ignore[attr-defined]
        raise


class JSONReceiver(ReadFileReceiver, WriteFileReceiver):
    """
    Receiver for JSON / NDJSON files with Pandas (bulk) and Dask (bigdata).
    """

    async def read_row(
        self, filepath: Path, metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Stream rows from JSON-like files.

        - NDJSON: use lenient iterator that skips malformed lines and bumps
          metrics.error_count.
        - JSON array/object: delegate to read_json_row() streaming iterator.
        """
        ensure_exists(filepath)

        if is_ndjson_path(filepath):

            def _on_error(_: Exception) -> None:
                metrics.error_count += 1

            it = iter_ndjson_lenient(filepath, on_error=_on_error)
        else:
            it = read_json_row(filepath)

        while True:
            rec = await asyncio.to_thread(_next_or_sentinel, it)
            if rec is _SENTINEL:
                break
            metrics.lines_received += 1
            yield rec

    async def read_bulk(
        self, filepath: Path, metrics: ComponentMetrics
    ) -> pd.DataFrame:
        """
        Read the entire file into a Pandas DataFrame.

        - NDJSON: use pandas.read_json(lines=True).
        - JSON array/object: use load_json_records() then DataFrame(records)
          to preserve mixed value types (e.g., keep "88" as str).
        """
        ensure_exists(filepath)

        if is_ndjson_path(filepath):

            def _read_ndjson() -> pd.DataFrame:
                p = str(filepath)
                return pd.read_json(p, lines=True, compression="infer")

            try:
                df = await asyncio.to_thread(_read_ndjson)
            except Exception as exc:
                metrics.error_count += 1
                raise FileReceiverError(
                    f"Failed to read NDJSON to Pandas: {exc}"
                ) from exc

            metrics.lines_received += len(df)
            return df

        # Non-NDJSON: keep original JSON value types by building
        # DataFrame from python objects
        def _read_array_or_object() -> pd.DataFrame:
            records = load_json_records(filepath)
            return pd.DataFrame.from_records(records)

        try:
            df = await asyncio.to_thread(_read_array_or_object)
        except Exception as exc:
            metrics.error_count += 1
            raise FileReceiverError(f"Failed to read JSON to Pandas: {exc}") from exc

        metrics.lines_received += len(df)
        return df

    async def read_bigdata(
        self, filepath: Path, metrics: ComponentMetrics
    ) -> dd.DataFrame:
        """
        Read large JSON/NDJSON using Dask.
        """
        ensure_exists(filepath)

        def _read() -> dd.DataFrame:
            p = str(filepath)
            if is_ndjson_path(filepath):
                return dd.read_json(
                    p, lines=True, blocksize="64MB", compression="infer"
                )
            return dd.read_json(
                p, orient="records", blocksize="64MB", compression="infer"
            )

        try:
            ddf = await asyncio.to_thread(_read)
        except Exception as exc:
            raise FileReceiverError(f"Failed to read JSON to Dask: {exc}") from exc

        try:
            row_count = await asyncio.to_thread(
                lambda: int(ddf.map_partitions(len).sum().compute())
            )
            metrics.lines_received += row_count
        except Exception:
            # Swallow counting errors to avoid blocking reads
            pass

        return ddf

    async def write_row(
        self, filepath: Path, metrics: ComponentMetrics, row: Dict[str, Any]
    ) -> None:
        """
        Append a single record.

        - For NDJSON: append one line (O(1)).
        - For array JSON: atomic read-modify-write (O(n)).
        """
        if is_ndjson_path(filepath):
            try:
                await asyncio.to_thread(append_ndjson_record, filepath, row)
            except Exception as exc:
                raise FileReceiverError(f"Failed to append NDJSON row: {exc}") from exc
            metrics.lines_received += 1
            return

        def _rmw() -> None:
            if filepath.exists():
                existing = load_json_records(filepath)
                existing.append(row)
                _atomic_overwrite(
                    filepath, lambda tmp: dump_json_records(tmp, existing, indent=2)
                )
            else:
                dump_json_records(filepath, [row], indent=2)

        try:
            await asyncio.to_thread(_rmw)
        except Exception as exc:
            raise FileReceiverError(f"Failed to write JSON row: {exc}") from exc

        metrics.lines_received += 1

    async def write_bulk(
        self, filepath: Path, metrics: ComponentMetrics, data: pd.DataFrame
    ) -> None:
        """
        Write many rows.

        - NDJSON: Pandas lines=True for streaming-friendly files.
        - JSON array: compact array-of-records.
        """

        def _write() -> None:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            p = str(filepath)
            if is_ndjson_path(filepath):
                data.to_json(
                    p,
                    orient="records",
                    lines=True,
                    force_ascii=False,
                    compression="infer",
                )
            else:
                data.to_json(
                    p,
                    orient="records",
                    indent=2,
                    force_ascii=False,
                    compression="infer",
                )

        try:
            await asyncio.to_thread(_write)
        except Exception as exc:
            raise FileReceiverError(f"Failed to write JSON bulk: {exc}") from exc

        metrics.lines_received += len(data)

    async def write_bigdata(
        self, filepath: Path, metrics: ComponentMetrics, data: dd.DataFrame
    ) -> None:
        """
        Write a Dask DataFrame as partitioned NDJSON files.

        - filepath denotes a directory; files written to part-*.jsonl.
        """

        def _count() -> int:
            return int(data.map_partitions(len).sum().compute())

        try:
            row_count = await asyncio.to_thread(_count)
        except Exception as exc:
            raise FileReceiverError(
                f"Failed to count rows; aborting write: {exc}"
            ) from exc

        def _write() -> None:
            filepath.mkdir(parents=True, exist_ok=True)
            data.to_json(
                str(filepath / "part-*.jsonl"),
                orient="records",
                lines=True,
                force_ascii=False,
                compression="infer",
            )

        try:
            await asyncio.to_thread(_write)
        except Exception as exc:
            raise FileReceiverError(f"Failed to write JSON: {exc}") from exc

        metrics.lines_received += row_count
