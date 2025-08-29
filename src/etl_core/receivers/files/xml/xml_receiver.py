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
    iter_xml_records,
    read_xml_bulk,
    read_xml_bigdata,
    write_xml_row,
    write_xml_bulk,
    write_xml_bigdata,
)

_SENTINEL: Any = object()


class XMLReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for XML files (supports nested structures)."""

    async def read_row(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            root_tag: str,
            record_tag: str,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Stream XML records sequentially in a worker thread."""
        ensure_file_exists(filepath)
        it = iter_xml_records(filepath, root_tag=root_tag, record_tag=record_tag)
        while True:
            row = await asyncio.to_thread(next, it, _SENTINEL)
            if row is _SENTINEL:
                break
            metrics.lines_received += 1
            yield row

    async def read_bulk(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            root_tag: str,
            record_tag: str,
    ) -> pd.DataFrame:
        ensure_file_exists(filepath)
        df = await asyncio.to_thread(read_xml_bulk, filepath, root_tag, record_tag)
        metrics.lines_received += len(df)
        return df

    async def read_bigdata(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            root_tag: str,
            record_tag: str,
    ) -> dd.DataFrame:
        ddf = await asyncio.to_thread(read_xml_bigdata, filepath, root_tag, record_tag)
        # counting rows safely (compute once)
        try:
            row_count = int(ddf.map_partitions(len).sum().compute())
        except Exception:
            row_count = 0
        metrics.lines_received += row_count
        return ddf

    async def write_row(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            row: Dict[str, Any],
            root_tag: str,
            record_tag: str,
    ):
        metrics.lines_received += 1
        await asyncio.to_thread(write_xml_row, filepath, row, root_tag, record_tag)
        metrics.lines_forwarded += 1

    async def write_bulk(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            data: pd.DataFrame,
            root_tag: str,
            record_tag: str,
    ):
        metrics.lines_received += len(data)
        await asyncio.to_thread(write_xml_bulk, filepath, data, root_tag, record_tag)
        metrics.lines_forwarded += len(data)

    async def write_bigdata(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            data: dd.DataFrame,
            root_tag: str,
            record_tag: str,
    ):
        try:
            data = await asyncio.to_thread(data.persist)
        except Exception as e:
            raise FileReceiverError(f"Failed to persist dataframe before write: {e}") from e

        try:
            row_count = await asyncio.to_thread(
                lambda: int(data.map_partitions(len).sum().compute())
            )
        except Exception as e:
            raise FileReceiverError(f"Failed to count rows; aborting write: {e}") from e

        try:
            await asyncio.to_thread(write_xml_bigdata, filepath, data, root_tag, record_tag)
        except Exception as e:
            raise FileReceiverError(f"Failed to write XML: {e}") from e

        metrics.lines_forwarded += row_count

