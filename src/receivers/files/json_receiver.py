# src/receivers/json_receiver.py

import json
from pathlib import Path
import pandas as pd
from typing import Any, Dict, List, Generator
from src.receivers.read_file_receiver import ReadFileReceiver
from src.receivers.write_file_receiver import WriteFileReceiver


class JSONReceiver(ReadFileReceiver, WriteFileReceiver):
    def read_row(self, filepath: Path) -> Dict[str, Any]:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data[0] if data else {}

    def read_bulk(self, filepath: Path) -> List[Dict[str, Any]]:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def read_bigdata(self, filepath: Path) -> Generator[Dict[str, Any], None, None]:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            for entry in data:
                yield entry

    def read_as_dataframe(self, filepath: Path) -> pd.DataFrame:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            return pd.DataFrame(data)

    def write_row(self, row: Dict[str, Any], filepath: Path):
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump([row], f, indent=2)

    def write_bulk(self, data: List[Dict[str, Any]], filepath: Path):
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def write_bigdata(self, chunk_iterable: Generator[Dict[str, Any], None, None], filepath: Path):
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(list(chunk_iterable), f, indent=2)

    def write_dataframe(self, df: pd.DataFrame, filepath: Path):
        df.to_json(filepath, orient="records", indent=2, force_ascii=False)