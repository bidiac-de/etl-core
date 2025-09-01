# File: etl_core/receivers/files/json/json_receiver.py
from __future__ import annotations
import asyncio
import contextlib
from pathlib import Path
from typing import Any, AsyncIterator, Dict

import dask.dataframe as dd
import pandas as pd

from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.file_helper import ensure_file_exists, FileReceiverError
from etl_core.receivers.files.read_file_receiver import ReadFileReceiver
from etl_core.receivers.files.write_file_receiver import WriteFileReceiver
from etl_core.receivers.files.json.json_helper import (
    is_ndjson_path,
    read_json_row,
    read_json_bulk_chunks,
    append_ndjson_record,
    dump_records_auto,
    iter_ndjson_lenient,
)

_SENTINEL: Any = object()


class JSONReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for JSON/NDJSON, aligned mit XML-Streaming:
    - read_row: async iterator of nested dicts
    - read_bulk: async iterator of pandas DataFrame *chunks* (column 'record')
    - read_bigdata: alias to read_bulk mit größerem chunk_size
    - write_bigdata: Single-File write (materialisiert als pandas)
    """

    async def read_row(self, filepath: Path, metrics: ComponentMetrics) -> AsyncIterator[Dict[str, Any]]:
        ensure_file_exists(filepath)
        if is_ndjson_path(filepath):
            def _on_error(_: Exception):
                metrics.error_count += 1
            it = iter_ndjson_lenient(filepath, on_error=_on_error)   # <--- mit Callback
        else:
            it = read_json_row(filepath)

        while True:
            rec = await asyncio.to_thread(next, it, _SENTINEL)
            if rec is _SENTINEL:
                break
            metrics.lines_forwarded += 1  # source component: forwarded statt received
            yield rec

    async def read_bulk(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            chunk_size: int = 10000,
    ) -> AsyncIterator[pd.DataFrame]:
        # Init-Phase absichern (auch hier Fehler zählen)
        try:
            ensure_file_exists(filepath)
            it = read_json_bulk_chunks(filepath, chunk_size=chunk_size)
        except Exception as exc:
            metrics.error_count += 1
            raise FileReceiverError(f"Failed to init JSON bulk reader: {exc}") from exc

        while True:
            try:
                df = await asyncio.to_thread(next, it, _SENTINEL)
            except Exception as exc:
                metrics.error_count += 1
                raise FileReceiverError(f"Failed to read JSON to Pandas: {exc}") from exc

            if df is _SENTINEL:
                break

            n = len(df)
            metrics.lines_forwarded += n   # read_* ist Source → forwarded, nicht received
            yield df

    async def read_bigdata(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            chunk_size: int = 50000,
    ) -> AsyncIterator[pd.DataFrame]:
        async for df in self.read_bulk(filepath, metrics, chunk_size=chunk_size):
            yield df

    async def write_row(self, filepath: Path, metrics: ComponentMetrics, *, row: Dict[str, Any]) -> None:
        try:
            if is_ndjson_path(filepath):
                await asyncio.to_thread(append_ndjson_record, filepath, row)
            else:
                # read-modify-write JSON array
                def _rmw():
                    import json
                    from .json_helper import open_text_auto
                    records: list[dict] = []
                    if filepath.exists():
                        with open_text_auto(filepath, "rt") as f:
                            txt = f.read().strip()
                            if txt:
                                if txt.startswith("["):
                                    records.extend(json.loads(txt))
                                else:
                                    obj = json.loads(txt)
                                    records.append(obj if isinstance(obj, dict) else {"_value": obj})
                    records.append(row)
                    dump_records_auto(filepath, records, indent=2)

                await asyncio.to_thread(_rmw)
        except Exception as exc:
            raise FileReceiverError(f"Failed to write JSON row: {exc}") from exc
        metrics.lines_received += 1
        metrics.lines_forwarded += 1

    async def write_bulk(self, filepath: Path, metrics: ComponentMetrics, *, data: pd.DataFrame) -> None:
        try:
            # support DataFrame with 'record' nested dicts or flat columns
            if "record" in data.columns:
                records = data["record"].tolist()
            else:
                records = [r._asdict() if hasattr(r, "_asdict") else dict(r) for _, r in data.iterrows()]
            await asyncio.to_thread(dump_records_auto, filepath, records, 2)
        except Exception as exc:
            raise FileReceiverError(f"Failed to write JSON bulk: {exc}") from exc
        n = len(data)
        metrics.lines_received += n
        metrics.lines_forwarded += n

    async def write_bigdata(self, filepath: Path, metrics: ComponentMetrics, *, data: dd.DataFrame) -> None:
        # materialize and reuse bulk write (single-file)
        try:
            pdf = await asyncio.to_thread(lambda: data.compute())
        except Exception as exc:
            raise FileReceiverError(f"Failed to materialize dask df: {exc}") from exc
        await self.write_bulk(filepath, metrics, data=pdf)

