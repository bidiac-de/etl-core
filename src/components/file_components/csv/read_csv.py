from pathlib import Path
from typing import Any, Dict, List, Generator
from src.components.file_components.csv.csv_component import CSV, Delimiter
from src.components.column_definition import ColumnDefinition
from receivers.files.csv_receiver import CSVReceiver


class ReadCSV(CSV):
    """Component that reads data from a CSV file."""

    def __init__(self,
                 id: int,
                 name: str,
                 description: str,
                 componentManager: Any,
                 filepath: Path,
                 schema_definition: List[ColumnDefinition],
                 separator: Delimiter = Delimiter.COMMA):
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

    def process_row(self, row: Dict[str, Any] = None) -> Dict[str, Any]:
        return self.receiver.read_row()

    def process_bulk(self, data: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        return self.receiver.read_bulk()

    def process_bigdata(self, chunk_iterable=None) -> Generator[Dict[str, Any], None, None]:
        return self.receiver.read_bigdata()