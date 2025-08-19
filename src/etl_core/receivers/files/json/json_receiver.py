from pathlib import Path
from typing import Any, Dict, List, AsyncIterator, Union
import pandas as pd
import dask.dataframe as dd
import asyncio

from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.read_file_receiver import ReadFileReceiver
from etl_core.receivers.files.write_file_receiver import WriteFileReceiver
from etl_core.receivers.files.file_helper import ensure_exists, FileReceiverError
from etl_core.receivers.files.json.json_helper import (
    load_json_records,
    dump_json_records,
    read_json_row,
)

_SENTINEL: Any = object()


def _next_or_sentinel(it):
    try:
        return next(it)
    except StopIteration:
        return _SENTINEL


class JSONReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for JSON / NDJSON files with Pandas (bulk) and Dask (bigdata)."""

    async def read_row(
            self, filepath: Path, metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        ensure_exists(filepath)
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
        Read the entire JSON/NDJSON file into a Pandas DataFrame.
        """
        ensure_exists(filepath)

        def _read() -> pd.DataFrame:
            p = str(filepath)
            if p.endswith((".jsonl", ".ndjson")):
                return pd.read_json(filepath, lines=True)
            return pd.read_json(filepath, orient="records")

        df = await asyncio.to_thread(_read)
        metrics.lines_received += len(df)
        return df

    async def read_bigdata(
            self, filepath: Path, metrics: ComponentMetrics
    ) -> dd.DataFrame:
        """
        Read large JSON/NDJSON files as a Dask DataFrame.
        """
        ensure_exists(filepath)

        def _read() -> dd.DataFrame:
            p = str(filepath)
            if p.endswith((".jsonl", ".ndjson", ".jsonl.gz", ".ndjson.gz")):
                return dd.read_json(p, lines=True, blocksize="64MB")
            return dd.read_json(p, orient="records", blocksize="64MB")

        ddf = await asyncio.to_thread(_read)

        try:
            row_count = await asyncio.to_thread(
                lambda: int(ddf.map_partitions(len).sum().compute())
            )
        except Exception as e:
            raise FileReceiverError(f"Failed to count rows for bigdata read: {e}") from e

        metrics.lines_received += row_count
        return ddf


    async def write_row(
            self, filepath: Path, metrics: ComponentMetrics, row: Dict[str, Any]
    ):
        """
        Append a single JSON record. For array-of-records JSON: read -> append -> write.
        For NDJSON, you could extend to write a single line.
        """

        def _write():
            if filepath.exists():
                existing = load_json_records(filepath)
                existing.append(row)
                dump_json_records(filepath, existing, indent=2)
            else:
                dump_json_records(filepath, [row], indent=2)

        try:
            await asyncio.to_thread(_write)
        except Exception as e:
            raise FileReceiverError(f"Failed to write JSON row: {e}") from e

        metrics.lines_received += 1

    async def write_bulk(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            data: pd.DataFrame
    ):
        """
        Write multiple rows to a JSON file (array-of-records).
        """

        def _write():
            data.to_json(filepath, orient="records", indent=2, force_ascii=False)

        try:
            await asyncio.to_thread(_write)
        except Exception as e:
            raise FileReceiverError(f"Failed to write JSON bulk: {e}") from e

        metrics.lines_received += len(data)


    async def write_bigdata(
            self, filepath: Path, metrics: ComponentMetrics, data: dd.DataFrame
    ):
        """
        Write a Dask DataFrame as partitioned JSON Lines files.
        """

        try:
            row_count = await asyncio.to_thread(
                lambda: int(data.map_partitions(len).sum().compute())
            )
        except Exception as e:
            raise FileReceiverError(f"Failed to count rows; aborting write: {e}") from e

        def _write():
            filepath.mkdir(parents=True, exist_ok=True)
            data.to_json(
                str(filepath / "part-*.jsonl"),
                orient="records",
                lines=True,
                force_ascii=False,
            )

        try:
            await asyncio.to_thread(_write)
        except Exception as e:
            raise FileReceiverError(f"Failed to write JSON: {e}") from e

        metrics.lines_received += row_count
