from pathlib import Path
from typing import Any, Dict, List, AsyncIterator, Union
import pandas as pd
import dask.dataframe as dd
import asyncio

from src.metrics.component_metrics import ComponentMetrics
from src.receivers.read_file_receiver import ReadFileReceiver
from src.receivers.write_file_receiver import WriteFileReceiver
from src.receivers.files.file_helper import resolve_file_path, ensure_exists
from src.receivers.files.json_helper import load_json_records, dump_json_records


class JSONReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for JSON / NDJSON files with Pandas (bulk) and Dask (bigdata)."""

    def __init__(self, filepath: Path):
        """
        Initialize the JSONReceiver with a fixed filepath.
        """
        super().__init__()
        self.filepath: Path = resolve_file_path(filepath)


    async def read_row(self, metrics: ComponentMetrics) -> AsyncIterator[Dict[str, Any]]:
        """
        Stream JSON rows as dictionaries (async generator).
        """
        ensure_exists(self.filepath)

        records: List[Dict[str, Any]] = await asyncio.to_thread(load_json_records, self.filepath)
        for rec in records:
            yield rec

    async def read_bulk(self, metrics: ComponentMetrics) -> pd.DataFrame:
        """
        Read the entire JSON/NDJSON file into a Pandas DataFrame.
        """
        ensure_exists(self.filepath)

        def _read() -> pd.DataFrame:
            p = str(self.filepath)
            if p.endswith((".jsonl", ".ndjson")):
                return pd.read_json(self.filepath, lines=True)
            return pd.read_json(self.filepath, orient="records")

        return await asyncio.to_thread(_read)

    async def read_bigdata(self, metrics: ComponentMetrics, blocksize: str = "64MB") -> dd.DataFrame:
        """
        Read large JSON/NDJSON files into a Dask DataFrame.
        """
        ensure_exists(self.filepath)

        def _read() -> dd.DataFrame:
            p = str(self.filepath)
            if p.endswith((".jsonl", ".ndjson")):
                return dd.read_json(p, lines=True, blocksize=blocksize)
            return dd.read_json(p, orient="records", blocksize=blocksize)

        return await asyncio.to_thread(_read)

    async def write_row(self, metrics: ComponentMetrics, row: Dict[str, Any]):
        """
        Write a single JSON record as a one-element array.
        """
        await asyncio.to_thread(dump_json_records, self.filepath, [row], 2)

    async def write_bulk(self, metrics: ComponentMetrics, data: Union[pd.DataFrame, List[Dict[str, Any]]]):
        """
        Write multiple rows to a JSON file.
        """
        def _write():
            if isinstance(data, pd.DataFrame):
                data.to_json(self.filepath, orient="records", indent=2, force_ascii=False)
            elif isinstance(data, list) and data:
                dump_json_records(self.filepath, data, 2)
        await asyncio.to_thread(_write)

    async def write_bigdata(self, metrics: ComponentMetrics, data: dd.DataFrame):
        """
        Write a Dask DataFrame as partitioned JSON Lines files.
        """
        def _write():
            self.filepath.mkdir(parents=True, exist_ok=True)
            data.to_json(str(self.filepath / "part-*.json"), orient="records", lines=True, force_ascii=False)
        await asyncio.to_thread(_write)

