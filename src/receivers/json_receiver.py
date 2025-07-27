# src/receivers/json_receiver.py

import json
from pathlib import Path
from typing import Any, Dict, List, Generator
from src.receivers.read_receiver import ReadReceiver
from src.receivers.write_receiver import WriteReceiver


class JSONReceiver(ReadReceiver, WriteReceiver):
    def __init__(self, filepath: Path):
        self.filepath = filepath

    def read_row(self) -> Dict[str, Any]:
        with open(self.filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data[0] if data else {}

    def read_bulk(self) -> List[Dict[str, Any]]:
        with open(self.filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def read_bigdata(self) -> Generator[Dict[str, Any], None, None]:
        with open(self.filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            for entry in data:
                yield entry

    def write_row(self, row: Dict[str, Any]):
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump([row], f, indent=2)

    def write_bulk(self, data: List[Dict[str, Any]]):
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def write_bigdata(self, chunk_iterable: Generator[Dict[str, Any], None, None]):
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(list(chunk_iterable), f, indent=2)