from abc import abstractmethod, ABC
from typing import List, Any, Iterable
from src.components.base_component import Component
from src.components.column_definition import ColumnDefinition
from src.components.databases.connection_handler import ConnectionHandler
from src.context.context import Context


class DatabaseComponent(Component, ABC):
    def __init__(
        self,
        id: int,
        name: str,
        description: str,
        context: Context,
        connection_handler: ConnectionHandler,
        schema_definition: List[ColumnDefinition],
    ):
        super().__init__(id=id, name=name, description=description, type="database")
        self._context = context
        self._connection_handler = connection_handler
        self._schema_definition = schema_definition

    @property
    def context(self) -> Context:
        return self._context

    @context.setter
    def context(self, value: Context):
        if not isinstance(value, Context):
            raise TypeError("context must be a Context instance")
        self._context = value

    @property
    def connection_handler(self) -> ConnectionHandler:
        return self._connection_handler

    @connection_handler.setter
    def connection_handler(self, value: ConnectionHandler):
        if not isinstance(value, ConnectionHandler):
            raise TypeError("connection_handler must be a ConnectionHandler instance")
        self._connection_handler = value

    @property
    def schema_definition(self) -> List[ColumnDefinition]:
        return self._schema_definition

    @schema_definition.setter
    def schema_definition(self, value: List[ColumnDefinition]):
        if not all(isinstance(c, ColumnDefinition) for c in value):
            raise TypeError(
                "schema_definition must be a list of ColumnDefinition instances"
            )
        self._schema_definition = value

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
