from pathlib import Path
from typing import Any, Dict, List
from src.components.file_components.csv.csv_component import CSV, Delimiter
from receivers.files.csv_receiver import CSVReceiver
from src.components.column_definition import ColumnDefinition


class WriteCSV(CSV):
    """Component that writes data to a CSV file."""

    def __init__(
            self,
            id: int,
            name: str,
            description: str,
            componentManager,
            filepath: Path,
            schema_definition: List[ColumnDefinition],
            separator: Delimiter = Delimiter.COMMA
    ):
        super().__init__(
            id=id,
            name=name,
            description=description,
            componentManager=componentManager,
            filepath=filepath,
            schema_definition=schema_definition,
            separator=separator
        )
        self.receiver = CSVReceiver(filepath, separator)

    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        self.receiver.write_row(row)
        return row

    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        self.receiver.write_bulk(data)
        return data

    def process_bigdata(self, chunk_iterable):
        self.receiver.write_bigdata(chunk_iterable)
        return chunk_iterable