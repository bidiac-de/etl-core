# base class for database operations
from typing import List
from src.components.base_components import Component
from src.components.column_definition import ColumnDefinition
from src.components.databases.connection_handler import ConnectionHandler
from src.context.credentials import Credentials




class DatabaseComponent(Component):
    def __init__(
        self,
        id: int,
        name: str,
        description: str,
        credentials: Credentials,
        connection_handler: ConnectionHandler,
        schema_definition: List[ColumnDefinition],
    ):
        super().__init__(
            id=id, name=name, description=description, type="database"
        )
        self.credentials = credentials
        self.connectionHandler = connection_handler
        self.schemaDefinition = schema_definition

    def process_row(self, row):
        print(f"[DatabaseComponent] processing row: {row}")
        return row

    def process_bulk(self, data):
        print("[DatabaseComponent] processing bulk data")
        return data

    def process_bigdata(self, chunk_iterable):
        print("[DatabaseComponent] processing big data stream")
        return list(chunk_iterable)
