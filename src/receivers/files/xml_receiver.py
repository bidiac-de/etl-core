from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List, AsyncIterator, Union
import asyncio
import xml.etree.ElementTree as ET
import pandas as pd
import dask.dataframe as dd

from src.receivers.files.read_file_receiver import ReadFileReceiver
from src.receivers.files.write_file_receiver import WriteFileReceiver
from src.receivers.files.file_helper import ensure_exists
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.xml_helper import (
    read_xml_bulk_records,
    append_xml_row,
    write_xml_full,
    ddf_write_partitioned_xml,
    dask_row_count
)

_SENTINEL: Any = object()

class XMLReceiver(ReadFileReceiver, WriteFileReceiver):
    """XML receiver (pandas bulk; simple Dask wrapper for big data)."""

    async def read_row(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            root_tag: str = "rows",
            record_tag: str = "row",
    ) -> AsyncIterator[Dict[str, Any]]:
        ensure_exists(filepath)

        def _read_all() -> List[Dict[str, Any]]:
            return read_xml_bulk_records(filepath, record_tag=record_tag)

        records = await asyncio.to_thread(_read_all)
        for rec in records:
            metrics.lines_received += 1
            yield rec

    async def read_bulk(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            root_tag: str = "rows",
            record_tag: str = "row",
    ) -> pd.DataFrame:
        ensure_exists(filepath)

        def _read_df() -> pd.DataFrame:
            rows = read_xml_bulk_records(filepath, record_tag=record_tag)
            if not rows:
                return pd.DataFrame()
            all_keys: set[str] = set()
            for r in rows:
                all_keys.update(r.keys())
            return pd.DataFrame(rows, columns=list(all_keys))

        df = await asyncio.to_thread(_read_df)
        metrics.lines_received += int(df.shape[0])
        return df

    async def read_bigdata(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            root_tag: str = "rows",
            record_tag: str = "row",
            npartitions: int = 2,
    ) -> dd.DataFrame:
        pdf = await self.read_bulk(filepath, metrics, root_tag=root_tag, record_tag=record_tag)
        ddf = dd.from_pandas(pdf, npartitions=max(1, npartitions))
        return ddf

    async def write_row(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            row: Dict[str, Any],
            root_tag: str = "rows",
            record_tag: str = "row",
    ):
        def _write():
            append_xml_row(filepath, row=row, root_tag=root_tag, record_tag=record_tag)

        await asyncio.to_thread(_write)
        metrics.lines_received += 1

    async def write_bulk(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            data: Union[List[Dict[str, Any]], pd.DataFrame],
            root_tag: str = "rows",
            record_tag: str = "row",
    ):
        def _write():
            if isinstance(data, pd.DataFrame):
                records = data.to_dict(orient="records")
            else:
                records = data or []
            write_xml_full(filepath, records=records, root_tag=root_tag, record_tag=record_tag)

        await asyncio.to_thread(_write)
        if isinstance(data, pd.DataFrame):
            metrics.lines_received += int(data.shape[0])
        elif isinstance(data, list):
            metrics.lines_received += len(data)
        else:
            metrics.lines_received += 0

    async def write_bigdata(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            data: dd.DataFrame,
            record_tag: str = "row",
    ):
        """
        Writes a Dask DataFrame partition-wise into multiple XML files (part-*.xml)
        in a target directory (similar to JSON part-*.json).
        """
        def _write_all():
            ddf_write_partitioned_xml(
                dirpath=Path(filepath),
                ddf=data,
                record_tag=record_tag,
                root_tag="rows",
            )

        await asyncio.to_thread(_write_all)

        metrics.lines_received += dask_row_count(data)
