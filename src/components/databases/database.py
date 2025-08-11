from abc import abstractmethod, ABC
from typing import List, Any, Iterable
from src.components.base_component import Component
from src.components.column_definition import ColumnDefinition
from src.components.databases.connection_handler import ConnectionHandler
from src.context.context import Context
from pydantic import Field


class DatabaseComponent(Component, ABC):
    context: Context = Field(default_factory=lambda: Context())
    connection_handler: ConnectionHandler = Field(
        default_factory=lambda: ConnectionHandler()
    )
    schema_definition: List[ColumnDefinition] = Field(default_factory=list)

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
