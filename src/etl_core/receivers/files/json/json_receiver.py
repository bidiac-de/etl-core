import asyncio
import contextlib
import os
import tempfile
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, TypeVar, List

import dask.dataframe as dd
import dask
from dask import delayed
import pandas as pd
import atexit
import shutil

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
    iter_ndjson_lenient,
    _write_part_ndjson,
    stream_json_array_to_ndjson,
    _flatten_partition,
    _has_flat_paths,
)

_SENTINEL: Any = object()
T = TypeVar("T")

_TEMP_DIRS: set[Path] = set()


def _register_temp_dir(p: Path) -> None:
    _TEMP_DIRS.add(p)


def _cleanup_temp_dirs():
    for p in list(_TEMP_DIRS):
        shutil.rmtree(p, ignore_errors=True)
        _TEMP_DIRS.discard(p)


atexit.register(_cleanup_temp_dirs)


def _atomic_overwrite(path: Path, writer: Callable[[Path], None]) -> None:
    """Write to temp file, then atomically replace target."""
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix
    with tempfile.NamedTemporaryFile(
        "w", delete=False, dir=str(path.parent), suffix=suffix
    ) as tmp:
        tmp_path = Path(tmp.name)
    try:
        writer(tmp_path)
        os.replace(tmp_path, path)
    finally:
        with contextlib.suppress(Exception):
            tmp_path.unlink(missing_ok=True)


class JSONReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for JSON / NDJSON with Pandas (bulk) and Dask (bigdata)."""

    async def read_row(
        self, filepath: Path, metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        ensure_file_exists(filepath)

        def _on_error(_: Exception):
            metrics.error_count += 1

        it = (
            iter_ndjson_lenient(
                filepath,
                on_error=_on_error,
            )
            if is_ndjson_path(filepath)
            else read_json_row(filepath)
        )

        while True:
            rec = await asyncio.to_thread(next, it, _SENTINEL)
            if rec is _SENTINEL:
                break
            nested = ensure_nested_for_read(rec)
            metrics.lines_forwarded += 1
            yield nested

    async def read_bulk(
        self, filepath: Path, metrics: ComponentMetrics
    ) -> pd.DataFrame:
        ensure_file_exists(filepath)
        try:
            if is_ndjson_path(filepath):
                from etl_core.receivers.files.json.json_helper import (
                    iter_ndjson_lenient,
                )

                def _on_error(_: Exception):
                    metrics.error_count += 1

                records_iter = iter_ndjson_lenient(filepath, on_error=_on_error)
                records: List[Dict[str, Any]] = await asyncio.to_thread(
                    list, records_iter
                )
            else:
                records = await asyncio.to_thread(load_json_records, filepath)

            flat_records = [
                flatten_record(
                    ensure_nested_for_read(r if isinstance(r, dict) else {"_value": r})
                )
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
        blocksize: str | None = "64MB",
    ) -> dd.DataFrame:
        ensure_file_exists(filepath)

        try:
            if filepath.is_dir():
                read_target = str(filepath / "*.json*")
                is_single_file = False
            else:
                is_single_file = True
                if is_ndjson_path(filepath):
                    read_target = str(filepath)

                else:
                    tmpdir = Path(tempfile.mkdtemp(prefix="etl_json_to_ndjson_"))
                    _register_temp_dir(tmpdir)
                    ndjson_path = tmpdir / (
                        filepath.stem
                        + ".jsonl"
                        + (".gz" if str(filepath).lower().endswith(".gz") else "")
                    )

                    def _on_conv_err(_: Exception):
                        metrics.error_count += 1

                    await asyncio.to_thread(
                        stream_json_array_to_ndjson, filepath, ndjson_path, _on_conv_err
                    )
                    read_target = str(ndjson_path)

            if is_single_file:

                def _preflatten_ndjson(src: Path, dst: Path) -> int:
                    count = 0

                    def _on_flat_err(_: Exception):
                        metrics.error_count += 1

                    for rec in iter_ndjson_lenient(src, on_error=_on_flat_err):
                        flat = flatten_record(
                            ensure_nested_for_read(
                                rec if isinstance(rec, dict) else {"_value": rec}
                            )
                        )
                        append_ndjson_record(dst, flat)
                        count += 1
                    return count

                flat_dir = Path(tempfile.mkdtemp(prefix="etl_flatten_ndjson_"))
                _register_temp_dir(flat_dir)
                flat_path = flat_dir / "flattened.jsonl"
                await asyncio.to_thread(
                    _preflatten_ndjson, Path(read_target), flat_path
                )
                read_target = str(flat_path)

                use_blocksize = blocksize
                ddf = dd.read_json(read_target, lines=True, blocksize=use_blocksize)

            else:
                use_blocksize = None if str(read_target).endswith(".gz") else blocksize
                ddf_raw = dd.read_json(read_target, lines=True, blocksize=use_blocksize)

                ddf = ddf_raw.map_partitions(_flatten_partition)

            def _compute_row_count() -> int:
                try:
                    counts = ddf.map_partitions(lambda pdf: len(pdf)).compute()
                    try:
                        return int(getattr(counts, "sum")())
                    except Exception:
                        return int(sum(counts))
                except Exception:
                    return 0

            total_rows = await asyncio.to_thread(_compute_row_count)
            metrics.lines_forwarded += total_rows

            return ddf

        except Exception as exc:
            metrics.error_count += 1
            raise FileReceiverError(f"Failed to read JSON bigdata: {exc}") from exc

    async def write_row(
        self, filepath: Path, metrics: ComponentMetrics, row: Dict[str, Any]
    ) -> None:
        if not isinstance(row, dict):
            raise TypeError(
                f"Row mode expects a dict payload, got {type(row).__name__}"
            )

        if _has_flat_paths(row):
            raise FileReceiverError(
                "Row mode expects a nested dict (no dotted or [i] keys). "
                "Use write_bulk/write_bigdata for flattened input or unflatten first."
            )

        try:
            if is_ndjson_path(filepath):
                await asyncio.to_thread(append_ndjson_record, filepath, row)
            else:

                def _rmw():
                    existing = load_json_records(filepath) if filepath.exists() else []
                    existing.append(row)
                    _atomic_overwrite(
                        filepath, lambda tmp: dump_json_records(tmp, existing, indent=2)
                    )

                await asyncio.to_thread(_rmw)
        except Exception as exc:
            metrics.error_count += 1
            raise FileReceiverError(f"Failed to write JSON row: {exc}") from exc

        metrics.lines_received += 1
        metrics.lines_forwarded += 1

    async def write_bulk(
        self, filepath: Path, metrics: ComponentMetrics, data: pd.DataFrame
    ) -> None:
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
        try:
            path_lower = str(filepath).lower()
            out_dir = (
                filepath
                if not filepath.suffix
                else filepath.parent / f"{filepath.stem}_parts"
            )
            out_dir.mkdir(parents=True, exist_ok=True)

            parts = data.to_delayed()
            use_gz = path_lower.endswith(".gz")
            ext = ".jsonl.gz" if use_gz else ".jsonl"

            tasks = []
            for i, dpart in enumerate(parts):
                part_path = str(out_dir / f"part-{i:05d}{ext}")
                tasks.append(delayed(_write_part_ndjson)(dpart, part_path))

            try:
                counts = await asyncio.to_thread(lambda: dask.compute(*tasks))
            except Exception as exc:
                metrics.error_count += 1
                raise FileReceiverError(f"Failed to write JSON bigdata: {exc}") from exc

            total = int(sum(counts))
            metrics.lines_received += total
            metrics.lines_forwarded += total

        except Exception as exc:
            metrics.error_count += 1
            raise FileReceiverError(f"Failed to write JSON bigdata: {exc}") from exc
