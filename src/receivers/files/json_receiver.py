from pathlib import Path
from typing import Any, Dict, List, AsyncIterator, Union
import pandas as pd
import dask.dataframe as dd
import asyncio

from src.metrics.component_metrics import ComponentMetrics
from src.receivers.read_file_receiver import ReadFileReceiver
from src.receivers.write_file_receiver import WriteFileReceiver
from src.receivers.files.file_helper import ensure_exists
from src.receivers.files.json_helper import load_json_records, dump_json_records


class JSONReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for JSON / NDJSON files with Pandas (bulk) and Dask (bigdata)."""

    async def read_row(
        self, filepath: Path, metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Stream JSON rows as dictionaries (async generator).
        """
        ensure_exists(filepath)

        records: List[Dict[str, Any]] = await asyncio.to_thread(
            load_json_records, filepath
        )
        for rec in records:
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

        return await asyncio.to_thread(_read)

    async def read_bigdata(
        self, filepath: Path, metrics: ComponentMetrics
    ) -> dd.DataFrame:
        """
        Read large JSON/NDJSON files as a Dask DataFrame.
        """
        ensure_exists(filepath)

        def _read() -> dd.DataFrame:
            p = str(filepath)
            if p.endswith((".jsonl", ".ndjson")):
                return dd.read_json(p, lines=True, blocksize="64MB")
            return dd.read_json(p, orient="records", blocksize="64MB")

        return await asyncio.to_thread(_read)

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

        await asyncio.to_thread(_write)

    async def write_bulk(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        data: Union[pd.DataFrame, List[Dict[str, Any]]],
    ):
        """
        Write multiple rows to a JSON file (array-of-records).
        """

        def _write():
            if isinstance(data, pd.DataFrame):
                data.to_json(filepath, orient="records", indent=2, force_ascii=False)
            elif isinstance(data, list) and data:
                dump_json_records(filepath, data, indent=2)
            else:
                dump_json_records(filepath, [], indent=2)

        await asyncio.to_thread(_write)

    async def write_bigdata(
        self, filepath: Path, metrics: ComponentMetrics, data: dd.DataFrame
    ):
        """
        Write a Dask DataFrame as partitioned JSON Lines files.
        """

        def _write():
            filepath.mkdir(parents=True, exist_ok=True)
            data.to_json(
                str(filepath / "part-*.json"),
                orient="records",
                lines=True,
                force_ascii=False,
            )

        await asyncio.to_thread(_write)
