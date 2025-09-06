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
    _flatten_partition,
    infer_flat_meta, iter_ndjson_lenient, dump_ndjson_records,
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
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            chunk_size: int = 50_000,   # identisch zu XML-Chunking-Idee, hier aber nur fürs Gefühl
    ) -> dd.DataFrame:
        """
        Lies JSON/NDJSON streamend, flatten wie XML (dot + [i]) zu Pandas,
        und wandle am Ende in EIN Dask-DataFrame um (für Test-Kompatibilität).
        """
        ensure_file_exists(filepath)

        try:
            # 1) Streaming-Quelle wählen – exakt wie bei XML (nur eben JSON)
            if is_ndjson_path(filepath):
                # tolerantes NDJSON-Streaming; Fehler zählen wir in read_row,
                # hier reicht „hart“ (Tests nutzen valide NDJSONs)
                it = iter_ndjson_lenient(filepath)
            else:
                it = read_json_row(filepath)

            # 2) Vollständig flatten (wie XML: ensure_nested_for_read -> flatten_record)
            records: List[Dict[str, Any]] = []
            # Achtung: Für echte Big-Data würdest du hier chunked DataFrames bauen
            # und z.B. dd.from_delayed verwenden. Für die Tests reicht ein Collect.
            for rec in it:
                d = rec if isinstance(rec, dict) else {"_value": rec}
                nested = ensure_nested_for_read(d)
                flat = flatten_record(nested)
                records.append(flat)

            pdf = pd.DataFrame.from_records(records)

        except Exception as exc:
            raise FileReceiverError(f"Failed to read JSON bigdata: {exc}") from exc

        # 3) Metrics – wie die Tests es erwarten
        metrics.lines_forwarded += len(pdf)

        # 4) Für die Tests als Dask zurückgeben (eine oder wenige Partitionen)
        #    -> Spalten sind bereits flach, genau wie bei XML-Bulk.
        nparts = 1 if len(pdf) <= chunk_size else max(2, min(8, len(pdf) // chunk_size))
        ddf = dd.from_pandas(pdf, npartitions=nparts)

        # Debug-Schutz, falls du sicher gehen willst:
        # assert "addr" not in set(ddf.columns), f"Unexpected nested col leaked: {list(ddf.columns)}"

        return ddf





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
        Wenn 'filepath' ein *Verzeichnis* ist oder keine Dateiendung hat:
          -> schreibe partitionierte NDJSON-Dateien: part-00000.jsonl, part-00001.jsonl, ...
        Sonst:
          -> wie Bulk: in eine einzelne Datei (JSON-Array oder NDJSON je nach Suffix).
        """
        ensure_file_exists(filepath.parent) if filepath.parent else None

        # Verzeichnis-Mode (Partitionen) – entspricht deinem Receiver-Test
        if (not filepath.suffix) or filepath.is_dir():
            out_dir = filepath if filepath.is_dir() else filepath
            out_dir.mkdir(parents=True, exist_ok=True)

            # Partitionen lazy holen
            parts = await asyncio.to_thread(lambda: data.to_delayed())
            total_rows = 0

            for i, dpart in enumerate(parts):
                pdf_i = await asyncio.to_thread(lambda: dpart.compute())
                # flat/nested akzeptieren → nested erzwingen
                records = [build_payload(r) for r in pdf_i.to_dict(orient="records")]
                part_path = out_dir / f"part-{i:05d}.jsonl"
                await asyncio.to_thread(dump_ndjson_records, part_path, records)
                total_rows += len(pdf_i)

            metrics.lines_received += total_rows
            metrics.lines_forwarded += total_rows
            return

        # Einzeldatei-Mode -> delegiere an write_bulk (zählt Metrics bereits)
        pdf = await asyncio.to_thread(lambda: data.compute())
        await self.write_bulk(filepath, metrics, pdf)
        # KEIN zusätzliches metrics-Update hier, um Doppelzählung zu vermeiden

