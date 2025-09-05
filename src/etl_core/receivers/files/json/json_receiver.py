import asyncio
import contextlib
import os
import tempfile
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, TypeVar, List

import dask.dataframe as dd
from dask.dataframe.utils import make_meta
import pandas as pd

from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.file_helper import FileReceiverError, ensure_file_exists
from etl_core.receivers.files.read_file_receiver import ReadFileReceiver
from etl_core.receivers.files.write_file_receiver import WriteFileReceiver
from etl_core.receivers.files.json.json_helper import (
    append_ndjson_record,
    dump_json_records,
    dump_records_auto,
    is_ndjson_path,
    load_json_records,
    read_json_row,
    build_payload,
    ensure_nested_for_read,
    flatten_record,
    _validate_node_json,
    flatten_partition,
    infer_flat_meta,
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
    """Receiver for JSON / NDJSON with Pandas (bulk) and Dask (bigdata).
    Nested handling mirrors XML semantics:
      - read_row -> yields nested dicts (unflattens if needed)
      - read_bulk -> returns Pandas DF with FLAT columns (dot / [i] paths)
      - write_row -> expects nested (rejects flat paths)
      - write_bulk/bigdata -> accept flat or nested; write nested
    """

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
            if not isinstance(rec, dict):
                rec = {"_value": rec}
            nested = ensure_nested_for_read(rec)
            metrics.lines_forwarded += 1
            yield nested

    async def read_bulk(
            self, filepath: Path, metrics: ComponentMetrics
    ) -> pd.DataFrame:
        """Return a FLAT DataFrame (columns are dot / [i] paths), matching XML bulk."""
        ensure_file_exists(filepath)
        try:
            # Load as list of dicts (robust to NDJSON / JSON array / single object)
            if is_ndjson_path(filepath):
                from etl_core.receivers.files.json.json_helper import iter_ndjson_lenient
                def _on_error(_: Exception):
                    metrics.error_count += 1
                records_iter = iter_ndjson_lenient(filepath, on_error=_on_error)
                records: List[Dict[str, Any]] = await asyncio.to_thread(list, records_iter)
            else:
                records = await asyncio.to_thread(load_json_records, filepath)

            # Normalize to nested & then flatten -> stable flat schema
            flat_records = [
                flatten_record(ensure_nested_for_read(r if isinstance(r, dict) else {"_value": r}))
                for r in records
            ]
            df = pd.DataFrame.from_records(flat_records)
        except Exception as exc:
            metrics.error_count += 1
            raise FileReceiverError(f"Failed to read JSON to Pandas: {exc}") from exc

        metrics.lines_forwarded += len(df)
        return df

    async def read_bigdata(
            self, filepath: Path, metrics: ComponentMetrics
    ) -> dd.DataFrame:
        ensure_file_exists(filepath)

        def _read() -> dd.DataFrame:
            p = str(filepath)
            if is_ndjson_path(filepath):
                return dd.read_json(p, lines=True, blocksize="64MB", compression="infer")
            return dd.read_json(p, orient="records", blocksize="64MB", compression="infer")

        try:
            ddf = await asyncio.to_thread(_read)
        except Exception as exc:
            raise FileReceiverError(f"Failed to read JSON to Dask: {exc}") from exc

        try:
            meta_df = await asyncio.to_thread(infer_flat_meta, ddf)
            meta = make_meta(meta_df)
        except Exception:
            meta = pd.DataFrame()

        try:
            ddf_flat = ddf.map_partitions(flatten_partition, meta=meta)
        except Exception as exc:
            raise FileReceiverError(f"Failed to flatten Dask partitions: {exc}") from exc

        with contextlib.suppress(Exception):
            count = await asyncio.to_thread(
                lambda: int(ddf_flat.map_partitions(len).sum().compute())
            )
            metrics.lines_forwarded += count

        return ddf_flat



    async def write_row(
            self, filepath: Path, metrics: ComponentMetrics, row: Dict[str, Any]
    ) -> None:
        _validate_node_json(row)
        try:
            if is_ndjson_path(filepath):
                await asyncio.to_thread(append_ndjson_record, filepath, row)
            else:
                def _rmw():
                    existing = load_json_records(filepath) if filepath.exists() else []
                    existing.append(row)
                    _atomic_overwrite(filepath, lambda tmp: dump_json_records(tmp, existing, indent=2))
                await asyncio.to_thread(_rmw)
        except Exception as exc:
            raise FileReceiverError(f"Failed to write JSON row: {exc}") from exc

        metrics.lines_received += 1
        metrics.lines_forwarded += 1

    async def write_bulk(
            self, filepath: Path, metrics: ComponentMetrics, data: pd.DataFrame
    ) -> None:
        # Build nested records first (flat → unflatten; drop nullish)
        records = []
        for _, r in data.iterrows():
            nested = build_payload(r.to_dict())
            records.append(nested)

        def _write_nested_records():
            filepath.parent.mkdir(parents=True, exist_ok=True)
            dump_records_auto(filepath, records, indent=2)

        try:
            await asyncio.to_thread(_write_nested_records)
        except Exception as exc:
            raise FileReceiverError(f"Failed to write JSON bulk: {exc}") from exc

        metrics.lines_received += len(data)
        metrics.lines_forwarded += len(data)

    async def write_bigdata(
            self, filepath: Path, metrics: ComponentMetrics, data: dd.DataFrame
    ) -> None:
        """
        Mirror XML semantics: compute to Pandas, then write nested.
        This ensures unflattening works for bigdata as well.
        """
        try:
            row_count = await asyncio.to_thread(lambda: int(data.map_partitions(len).sum().compute()))
        except Exception as exc:
            raise FileReceiverError(f"Failed to count rows; aborting write: {exc}") from exc

        # Compute now and reuse write_bulk path
        pdf = await asyncio.to_thread(lambda: data.compute())
        await self.write_bulk(filepath, metrics, pdf)

        metrics.lines_forwarded += row_count
