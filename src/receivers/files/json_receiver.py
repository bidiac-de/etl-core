import json
from pathlib import Path
import pandas as pd
import dask.dataframe as dd
from typing import Any, Dict, List, Generator

from metrics.component_metrics import ComponentMetrics
from src.receivers.read_file_receiver import ReadFileReceiver
from src.receivers.write_file_receiver import WriteFileReceiver


class JSONReceiver(ReadFileReceiver, WriteFileReceiver):
    def read_row(self, filepath: Path, metrics: ComponentMetrics) -> Dict[str, Any]:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data[0] if data else {}

    def read_bulk(self, filepath: Path, metrics: ComponentMetrics) -> pd.DataFrame:
        """Reads a JSON file into a pandas DataFrame."""
        return pd.read_json(filepath, orient="records")

    def read_bigdata(self, filepath: Path, metrics: ComponentMetrics) -> dd.DataFrame:
        """Reads a large JSON file into a Dask DataFrame."""
        return dd.read_json(str(filepath), orient="records", blocksize="64MB")


    def write_row(self, row: Dict[str, Any], filepath: Path, metrics: ComponentMetrics): # Format für eine row?
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump([row], f, indent=2)

    def write_bulk(self, df: pd.DataFrame, filepath: Path, metrics: ComponentMetrics):
        """Writes a pandas DataFrame to a JSON file."""
        df.to_json(filepath, orient="records", indent=2, force_ascii=False)

    def write_bigdata(self, ddf: dd.DataFrame, filepath: Path, metrics: ComponentMetrics):
        """Writes a Dask DataFrame to JSON (into folder of partitions)."""
        ddf.to_json(str(filepath / "part-*.json"), orient="records", lines=True, force_ascii=False)

