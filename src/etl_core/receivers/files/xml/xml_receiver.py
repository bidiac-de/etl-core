from pathlib import Path
from typing import Dict, Any, AsyncIterator
import pandas as pd
import dask.dataframe as dd
import asyncio

from etl_core.receivers.files.file_helper import ensure_file_exists, FileReceiverError
from etl_core.receivers.files.read_file_receiver import ReadFileReceiver
from etl_core.receivers.files.write_file_receiver import WriteFileReceiver
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.xml.xml_helper import (
    read_xml_row,
    read_xml_bulk_chunks,
    write_xml_row,
    write_xml_bulk,
)


_SENTINEL: Any = object()


class XMLReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for XML files (nested read/write supported). All reads are async *streams*.
    - read_row -> async iterator of dict records
    - read_bulk -> async iterator of pandas DataFrame *chunks*
    - read_bigdata -> alias to read_bulk with (potentially) different chunk_size
    """

    async def read_row(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            record_tag: str,
    ) -> AsyncIterator[Dict[str, Any]]:
        ensure_file_exists(filepath)
        it = read_xml_row(filepath, record_tag=record_tag)
        while True:
            rec = await asyncio.to_thread(next, it, _SENTINEL)
            if rec is _SENTINEL:
                break
            metrics.lines_forwarded += 1
            yield rec

    async def read_bulk(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            record_tag: str,
            chunk_size: int = 10000,
    ) -> AsyncIterator[pd.DataFrame]:
        ensure_file_exists(filepath)
        it = read_xml_bulk_chunks(filepath, record_tag=record_tag, chunk_size=chunk_size)
        while True:
            df = await asyncio.to_thread(next, it, _SENTINEL)
            if df is _SENTINEL:
                break
            n = len(df)
            metrics.lines_forwarded += n
            yield df

    async def read_bigdata(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            record_tag: str,
            chunk_size: int = 50000,
    ) -> AsyncIterator[pd.DataFrame]:
        async for df in self.read_bulk(
                filepath, metrics, record_tag=record_tag, chunk_size=chunk_size
        ):
            yield df

    async def write_row(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            row: Dict[str, Any],
            root_tag: str,
            record_tag: str,
    ) -> None:
        metrics.lines_received += 1
        await asyncio.to_thread(write_xml_row, filepath, row, root_tag=root_tag, record_tag=record_tag)
        metrics.lines_forwarded += 1

    async def write_bulk(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            data: pd.DataFrame,
            root_tag: str,
            record_tag: str,
    ) -> None:
        n = len(data)
        metrics.lines_received += n
        await asyncio.to_thread(write_xml_bulk, filepath, data, root_tag=root_tag, record_tag=record_tag)
        metrics.lines_forwarded += n

    async def write_bigdata(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            data: dd.DataFrame,
            root_tag: str,
            record_tag: str,
    ) -> None:
        try:
            row_count = await asyncio.to_thread(
                lambda: int(data.map_partitions(len).sum().compute())
            )
        except Exception:
            row_count = None

        pdf = await asyncio.to_thread(lambda: data.compute())

        metrics.lines_received += len(pdf)
        await asyncio.to_thread(
            write_xml_bulk, filepath, pdf, root_tag=root_tag, record_tag=record_tag
        )
        metrics.lines_forwarded += row_count if row_count is not None else len(pdf)


