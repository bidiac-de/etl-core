# base class for database operations
from abc import abstractmethod
from typing import List, Any, Iterable
from src.components.base_components import Component
from src.components.column_definition import ColumnDefinition
from src.components.databases.connection_handler import ConnectionHandler
from src.context.credentials import Credentials
from src.context.context import Context




class DatabaseComponent(Component):
    def __init__(
        self,
        id: int,
        name: str,
        description: str,
        context: Context,
        connection_handler: ConnectionHandler,
        schema_definition: List[ColumnDefinition],
    ):
        super().__init__(
            id=id, name=name, description=description, type="database"
        )
        self.context = context
        self.connectionHandler = connection_handler
        self.schemaDefinition = schema_definition

    @abstractmethod
    def process_row(self, row) -> Any:
        """Process a single row. Implement in subclass."""
        raise NotImplementedError

    @abstractmethod
    def process_bulk(self, data) -> List[Any]:
        """Process an in-memory batch. Implement in subclass."""
        raise NotImplementedError

    @abstractmethod
    def process_bigdata(self, chunk_iterable) -> Iterable[Any]:
        """
        Stream-processing for big data. Implement in subclass.
        Should be a generator to avoid materializing large data.
        """
        raise NotImplementedError
