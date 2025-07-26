import csv
from typing import Dict, Any, List, Generator
from src.receivers.read_receiver import ReadReceiver
from src.receivers.write_receiver import WriteReceiver


class CSVReceiver(ReadReceiver, WriteReceiver):
    def __init__(self, file_path: str, id: int = 0):
        super().__init__(id)
        self.file_path = file_path

    def read_row(self) -> Dict[str, Any]:
        with open(self.file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                return row

    def read_bulk(self) -> List[Dict[str, Any]]:
        with open(self.file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            return list(reader)

    def read_bigdata(self) -> Generator[Dict[str, Any], None, None]:
        with open(self.file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                yield row

    def write_row(self, row: Dict[str, Any]):
        with open(self.file_path, mode='a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=row.keys())
            writer.writerow(row)

    def write_bulk(self, data: List[Dict[str, Any]]):
        if not data:
            return
        with open(self.file_path, mode='w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

    def write_bigdata(self, chunk_iterable: Generator[Dict[str, Any], None, None]):
        first_row = next(chunk_iterable, None)
        if not first_row:
            return
        with open(self.file_path, mode='w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=first_row.keys())
            writer.writeheader()
            writer.writerow(first_row)
            for row in chunk_iterable:
                writer.writerow(row)