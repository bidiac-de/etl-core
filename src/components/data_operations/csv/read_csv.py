from src.receivers.csv_receiver import CSVReceiver
from typing import Dict, Any, List, Generator
from src.components.data_operations.csv.csv_component import Delimiter
import csv

class ReadCSV(CSVReceiver):
    def __init__(self, filepath: str, id: int, name: str, description: str, componentManager: Any, separator: Delimiter = Delimiter.COMMA):
        self.id = id
        self.name = name
        self.description = description
        self.componentManager = componentManager
        self.filepath = filepath
        self.separator = separator

    def process_row(self) -> Dict[str, Any]:
        with open(self.filepath, newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=self.separator.value)
            return next(reader)

    def process_bulk(self) -> List[Dict[str, Any]]:
        with open(self.filepath, newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=self.separator.value)
            return list(reader)

    def process_bigdata(self) -> Generator[Dict[str, Any], None, None]:
        with open(self.filepath, newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=self.separator.value)
            for row in reader:
                yield row