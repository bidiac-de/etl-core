from __future__ import annotations
import asyncio
import contextlib
import tempfile
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, TypeVar
import dask.dataframe as dd
import pandas as pd

from etl_core.receivers.files.xml.xml_helper import Schema, iter_xml_records, validate_record, write_records_as_xml


class FileReceiverError(Exception):
    pass

_S = object()  # sentinel
T = TypeVar("T")

class XMLReceiver:
    """Receiver for XML with streaming row/bulk/bigdata, nested dict support, and schema validation."""

    def __init__(self, schema: Optional[Schema] = None) -> None:
        self._schema = schema

    # ----------------- READ -----------------
    async def read_row(
            self,
            filepath: Path,
            *,
            metrics: Any,
            record_tag: str,
    ) -> AsyncIterator[Dict[str, Any]]:
        def _on_error(_: Exception):
            with contextlib.suppress(Exception):
                metrics.error_count += 1

        it = iter_xml_records(filepath, record_tag=record_tag, on_error=_on_error)
        while True:
            rec = await asyncio.to_thread(next, it, _S)
            if rec is _S:
                break
            try:
                rec = validate_record(rec, self._schema)
            except Exception:
                with contextlib.suppress(Exception):
                    metrics.error_count += 1
                raise
            with contextlib.suppress(Exception):
                metrics.lines_received += 1
            yield rec

    async def read_bulk(
            self,
            filepath: Path,
            *,
            metrics: Any,
            root_tag: str,
            record_tag: str,
    ) -> pd.DataFrame:
        # stream -> list -> DataFrame (keeps memory moderate for medium files)
        try:
            records: List[Dict[str, Any]] = []
            async for rec in self.read_row(filepath, metrics=metrics, root_tag=root_tag, record_tag=record_tag):
                records.append(rec)
            df = pd.DataFrame.from_records(records)
        except Exception as exc:
            raise FileReceiverError(f"Failed to read XML to Pandas: {exc}") from exc
        with contextlib.suppress(Exception):
            metrics.lines_received += len(df)
        return df

    async def read_bigdata(
            self,
            filepath: Path,
            *,
            metrics: Any,
            root_tag: str,
            record_tag: str,
            chunk_size: int = 100_000,
    ) -> dd.DataFrame:
        """Convert XML to temporary partitioned NDJSON under the hood, then read with Dask.
        This avoids holding the entire XML in memory and provides parallelism.
        """
        tmpdir = Path(tempfile.mkdtemp(prefix="xml2jsonl_"))
        part_index = 0
        buf: List[Dict[str, Any]] = []

        try:
            async for rec in self.read_row(filepath, metrics=metrics, root_tag=root_tag, record_tag=record_tag):
                buf.append(rec)
                if len(buf) >= chunk_size:
                    part = tmpdir / f"part-{part_index:05d}.jsonl"
                    await asyncio.to_thread(_write_jsonl, part, buf)
                    buf.clear()
                    part_index += 1
            # tail
            if buf:
                part = tmpdir / f"part-{part_index:05d}.jsonl"
                await asyncio.to_thread(_write_jsonl, part, buf)
                buf.clear()

            ddf = dd.read_json(str(tmpdir / "part-*.jsonl"), lines=True, blocksize=None)
        except Exception as exc:
            raise FileReceiverError(f"Failed to read XML to Dask: {exc}") from exc

        return ddf

    # ----------------- WRITE -----------------
    async def write_row(
            self,
            filepath: Path,
            *,
            metrics: Any,
            row: Dict[str, Any],
            root_tag: str,
            record_tag: str,
    ) -> None:
        try:
            row = validate_record(row, self._schema)
            # read existing -> append -> atomic overwrite
            existing: List[Dict[str, Any]] = []
            if filepath.exists():
                # stream existing file for memory efficiency
                for rec in iter_xml_records(filepath, record_tag=record_tag):
                    existing.append(rec)
            existing.append(row)
            await asyncio.to_thread(write_records_as_xml, filepath, existing, root_tag=root_tag, record_tag=record_tag)
        except Exception as exc:
            raise FileReceiverError(f"Failed to write XML row: {exc}") from exc
        with contextlib.suppress(Exception):
            metrics.lines_received += 1

    async def write_bulk(
            self,
            filepath: Path,
            *,
            metrics: Any,
            data: pd.DataFrame | List[Dict[str, Any]],
            root_tag: str,
            record_tag: str,
    ) -> None:
        try:
            if isinstance(data, pd.DataFrame):
                records = data.to_dict(orient="records")
            else:
                records = data
            # validate
            if self._schema:
                records = [validate_record(r, self._schema) for r in records]
            await asyncio.to_thread(write_records_as_xml, filepath, records, root_tag=root_tag, record_tag=record_tag)
        except Exception as exc:
            raise FileReceiverError(f"Failed to write XML bulk: {exc}") from exc
        with contextlib.suppress(Exception):
            metrics.lines_received += len(records)  # type: ignore[name-defined]

    async def write_bigdata(
            self,
            filepath: Path,
            *,
            metrics: Any,
            data: dd.DataFrame,
            record_tag: str,
            root_tag: str = "rows",
    ) -> None:
        """Write a Dask DataFrame partition-wise to part-*.xml in a directory."""
        # count rows first
        try:
            nrows = await asyncio.to_thread(lambda: int(data.map_partitions(len).sum().compute()))
        except Exception as exc:
            raise FileReceiverError(f"Failed to count Dask rows: {exc}") from exc

        def _write_partitions():
            Path(filepath).mkdir(parents=True, exist_ok=True)
            nparts = data.npartitions
            for i in range(nparts):
                pdf = data.get_partition(i).compute()
                records = pdf.to_dict(orient="records")
                if self._schema:
                    records[:] = [validate_record(r, self._schema) for r in records]
                out_file = Path(filepath) / f"part-{i:05d}.xml"
                write_records_as_xml(out_file, records, root_tag=root_tag, record_tag=record_tag)

        try:
            await asyncio.to_thread(_write_partitions)
        except Exception as exc:
            raise FileReceiverError(f"Failed to write XML bigdata: {exc}") from exc

        with contextlib.suppress(Exception):
            metrics.lines_received += nrows

# helpers for read_bigdata
import json as _json

def _write_jsonl(path: Path, records: List[Dict[str, Any]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(_json.dumps(r, ensure_ascii=False))
            f.write("\n")
