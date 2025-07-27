from src.receivers.csv_receiver import CSVReceiver
from typing import Dict, Any, List, Generator
import csv
import os

class WriteCSV(CSVReceiver):
    def __init__(self, filepath: str, fieldnames: List[str], **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath
        self.fieldnames = fieldnames

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