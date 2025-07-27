from src.receivers.csv_receiver import CSVReceiver
from typing import Dict, Any, List, Generator
from src.components.data_operations.csv.csv_component import Delimiter
import csv
import os

class WriteCSV(CSVReceiver):
    def __init__(self, filepath: str, fieldnames: List[str], id: int, name: str, description: str, componentManager: Any, separator: Delimiter = Delimiter.COMMA):
        self.id = id
        self.name = name
        self.description = description
        self.componentManager = componentManager
        self.filepath = filepath
        self.fieldnames = fieldnames
        self.separator = separator

    def _get_writer(self):
        file_exists = os.path.exists(self.filepath)
        csvfile = open(self.filepath, 'a', newline='')
        writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames, delimiter=self.separator.value)
        if not file_exists:
            writer.writeheader()
        return writer, csvfile

    def process_row(self, row: Dict[str, Any]):
        writer, csvfile = self._get_writer()
        writer.writerow(row)
        csvfile.close()

    def process_bulk(self, data: List[Dict[str, Any]]):
        writer, csvfile = self._get_writer()
        writer.writerows(data)
        csvfile.close()

    def process_bigdata(self, chunk_iterable: Generator[Dict[str, Any], None, None]):
        writer, csvfile = self._get_writer()
        for row in chunk_iterable:
            writer.writerow(row)
        csvfile.close()