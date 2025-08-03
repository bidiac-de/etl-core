import json
from pathlib import Path
import pandas as pd
import dask.dataframe as dd
from typing import Any, Dict, List, Generator

from metrics.component_metrics import ComponentMetrics
from src.receivers.read_file_receiver import ReadFileReceiver
from src.receivers.write_file_receiver import WriteFileReceiver


from src.receivers.files.json_helper import load_json_records, dump_json_records

class JSONReceiver(ReadFileReceiver, WriteFileReceiver):
    def read_row(self, filepath: Path, metrics: ComponentMetrics) -> Dict[str, Any]:
        """Read a single record from a JSON file (first record if multiple)."""
        records = load_json_records(filepath)
        return records[0] if records else {}

    def read_bulk(self, filepath: Path, metrics: ComponentMetrics) -> pd.DataFrame:
        """Read a JSON file into a pandas DataFrame.

        Automatically detects JSON Lines (NDJSON) by file extension.
        """
        p = str(filepath)
        if p.endswith(".jsonl") or p.endswith(".ndjson"):
            return pd.read_json(filepath, lines=True)
        return pd.read_json(filepath, orient="records")

    def read_bigdata(self, filepath: Path, metrics: ComponentMetrics) -> dd.DataFrame:
        """Read a large JSON file into a Dask DataFrame.

        Uses JSON Lines mode when the file extension indicates NDJSON.
        """
        p = str(filepath)
        if p.endswith(".jsonl") or p.endswith(".ndjson"):
            return dd.read_json(p, lines=True, blocksize="64MB")
        return dd.read_json(p, orient="records", blocksize="64MB")

    def write_row(self, row: Dict[str, Any], filepath: Path, metrics: ComponentMetrics):
        """Write a single record as a one-element JSON array for consistency with bulk format."""
        dump_json_records(filepath, [row], indent=2)

    def write_bulk(self, df: pd.DataFrame, filepath: Path, metrics: ComponentMetrics):
        """Write a pandas DataFrame as a JSON array (records orientation)."""
        df.to_json(filepath, orient="records", indent=2, force_ascii=False)

    def write_bigdata(self, ddf: dd.DataFrame, filepath: Path, metrics: ComponentMetrics):
        """Write a Dask DataFrame as partitioned JSON Lines files (folder with part-*.json)."""
        (filepath).mkdir(parents=True, exist_ok=True)
        ddf.to_json(str(filepath / "part-*.json"), orient="records", lines=True, force_ascii=False)

