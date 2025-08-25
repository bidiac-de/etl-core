import asyncio
import contextlib
import os
import tempfile
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, TypeVar

import dask.dataframe as dd
import pandas as pd

from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.file_helper import FileReceiverError, ensure_file_exists
from etl_core.receivers.files.read_file_receiver import ReadFileReceiver
from etl_core.receivers.files.write_file_receiver import WriteFileReceiver
from etl_core.receivers.files.json.json_helper import (
    append_ndjson_record,
    dump_json_records,
    is_ndjson_path,
    load_json_records,
    read_json_row,
)

_SENTINEL: Any = object()
T = TypeVar("T")


def _atomic_overwrite(path: Path, writer: Callable[[Path], None]) -> None:
    """Write to temp file then atomically replace target."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent)) as tmp:
        tmp_path = Path(tmp.name)
    try:
        writer(tmp_path)
        os.replace(tmp_path, path)
    finally:
        with contextlib.suppress(Exception):
            tmp_path.unlink(missing_ok=True)


class JSONReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for JSON / NDJSON with Pandas (bulk) and Dask (bigdata)."""

    async def read_row(
        self, filepath: Path, metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        ensure_file_exists(filepath)

        if is_ndjson_path(filepath):
            from etl_core.receivers.files.json.json_helper import iter_ndjson_lenient

            def _on_error(_: Exception):
                metrics.error_count += 1

            it = iter_ndjson_lenient(filepath, on_error=_on_error)
        else:
            it = read_json_row(filepath)

        while True:
            rec = await asyncio.to_thread(next, it, _SENTINEL)
            if rec is _SENTINEL:
                break
            metrics.lines_received += 1
            yield rec

    async def read_bulk(
        self, filepath: Path, metrics: ComponentMetrics
    ) -> pd.DataFrame:
        ensure_file_exists(filepath)
        try:
            if is_ndjson_path(filepath):
                df = await asyncio.to_thread(
                    pd.read_json, str(filepath), lines=True, compression="infer"
                )
            else:
                records = await asyncio.to_thread(load_json_records, filepath)
                df = pd.DataFrame.from_records(records)
        except Exception as exc:
            metrics.error_count += 1
            raise FileReceiverError(f"Failed to read JSON to Pandas: {exc}") from exc

        metrics.lines_received += len(df)
        return df

    async def read_bigdata(
        self, filepath: Path, metrics: ComponentMetrics
    ) -> dd.DataFrame:
        ensure_file_exists(filepath)

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

        with contextlib.suppress(Exception):
            count = await asyncio.to_thread(
                lambda: int(ddf.map_partitions(len).sum().compute())
            )
            metrics.lines_received += count

        return ddf

    async def write_row(
        self, filepath: Path, metrics: ComponentMetrics, row: Dict[str, Any]
    ) -> None:
        try:
            if is_ndjson_path(filepath):
                await asyncio.to_thread(append_ndjson_record, filepath, row)
            else:

                def _rmw():
                    existing = load_json_records(filepath) if filepath.exists() else []
                    existing.append(row)
                    _atomic_overwrite(
                        filepath, lambda tmp: dump_json_records(tmp, existing, indent=2)
                    )

                await asyncio.to_thread(_rmw)
        except Exception as exc:
            raise FileReceiverError(f"Failed to write JSON row: {exc}") from exc

        metrics.lines_received += 1

    async def write_bulk(
        self, filepath: Path, metrics: ComponentMetrics, data: pd.DataFrame
    ) -> None:
        def _write():
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
        try:
            row_count = await asyncio.to_thread(
                lambda: int(data.map_partitions(len).sum().compute())
            )
        except Exception as exc:
            raise FileReceiverError(
                f"Failed to count rows; aborting write: {exc}"
            ) from exc

        def _write():
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
