from pathlib import Path
from typing import Dict, Any, AsyncIterator
import pandas as pd
import dask.dataframe as dd
import asyncio

from dask import delayed, compute

from etl_core.receivers.files.file_helper import ensure_file_exists, open_file
from etl_core.receivers.files.read_file_receiver import ReadFileReceiver
from etl_core.receivers.files.write_file_receiver import WriteFileReceiver
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.xml.xml_helper import (
    read_xml_row,
    write_xml_row,
    iter_pdf_chunks_core,
    render_rows_to_xml_fragment,
    wrap_with_root,
)


_SENTINEL: Any = object()


class XMLReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for XML files with nested read/write.

    All reads are async *streams*:
    - read_row -> yields dict records
    - read_bulk -> yields pandas DataFrame chunks
    - read_bigdata -> alias to read_bulk (potentially different chunk size)
    """

    async def read_row(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        record_tag: str = "row",
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
        record_tag: str = "row",
        chunk_size: int = 10_000,
    ) -> AsyncIterator[pd.DataFrame]:
        ensure_file_exists(filepath)
        it = iter_pdf_chunks_core(
            filepath, record_tag=record_tag, chunk_size=chunk_size
        )

        try:
            while True:
                df = await asyncio.to_thread(next, it, _SENTINEL)
                if df is _SENTINEL:
                    break
                metrics.lines_forwarded += len(df)
                yield df
        finally:
            close = getattr(it, "close", None)
            if callable(close):
                close()

    async def read_bigdata(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        record_tag: str = "row",
        chunk_size: int = 50_000,
    ) -> AsyncIterator[dd.DataFrame]:
        ensure_file_exists(filepath)
        it = iter_pdf_chunks_core(
            filepath, record_tag=record_tag, chunk_size=chunk_size
        )

        try:
            while True:
                pdf = await asyncio.to_thread(next, it, _SENTINEL)
                if pdf is _SENTINEL:
                    break
                metrics.lines_forwarded += len(pdf)

                ddf = dd.from_pandas(pdf, npartitions=1)
                yield ddf
        finally:
            close = getattr(it, "close", None)
            if callable(close):
                close()

    async def write_row(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        row: Dict[str, Any],
        root_tag: str = "rows",
        record_tag: str = "row",
    ) -> None:
        metrics.lines_received += 1
        await asyncio.to_thread(
            write_xml_row, filepath, row, root_tag=root_tag, record_tag=record_tag
        )
        metrics.lines_forwarded += 1

    async def write_bulk(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        data: pd.DataFrame,
        root_tag: str = "rows",
        record_tag: str = "row",
    ) -> None:
        n = len(data)
        metrics.lines_received += n

        cnt, frag = await asyncio.to_thread(
            render_rows_to_xml_fragment, data, record_tag
        )
        xml_text = wrap_with_root(frag, root_tag)

        with open_file(filepath, "w") as f:
            f.write(xml_text)

        metrics.lines_forwarded += cnt

    async def write_bigdata(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        data: dd.DataFrame,
        root_tag: str = "rows",
        record_tag: str = "row",
    ) -> None:
        if filepath.is_dir() or not filepath.suffix:
            raise ValueError(
                "write_bigdata requires a *file* path with extension (e.g. out.xml). "
                "Directory paths or paths without filename are not allowed."
            )

        filepath.parent.mkdir(parents=True, exist_ok=True)

        parts = data.to_delayed()

        total_cnt = 0
        with open_file(filepath, "w") as f:
            f.write(f'<?xml version="1.0" encoding="utf-8"?>\n<{root_tag}>')

            for part in parts:
                cnt, frag = await asyncio.to_thread(
                    lambda p=part: compute(
                        delayed(render_rows_to_xml_fragment)(p, record_tag)
                    )[0]
                )
                f.write(frag)
                total_cnt += cnt

            f.write(f"</{root_tag}>")

        metrics.lines_received += total_cnt
        metrics.lines_forwarded += total_cnt
