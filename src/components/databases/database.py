# base class for database operations
from typing import List
from components.base import Component
from src.components.column_definition import ColumnDefinition

class Credentials:
    def _init_(self, user: str, password: str, host: str, port: int):
        self.user = user
        self.password = password
        self.host = host
        self.port = port

class ConnectionHandler:
    def connect(self):
        print("[ConnectionHandler] Connecting to database")

    def disconnect(self):
        print("[ConnectionHandler] Disconnecting")

class DatabaseComponent(Component):
    def _init_(
            self,
            id: int,
            name: str,
            description: str,
            credentials: Credentials,
            connection_handler: ConnectionHandler,
            schema_definition: List[ColumnDefinition]
    ):
        super()._init_(
            id=id,
            name=name,
            description=description,
            comp_type="database"
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

