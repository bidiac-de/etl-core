from src.receivers.read_database_receiver import ReadDatabaseReceiver
from src.receivers.write_database_receiver import WriteDatabaseReceiver
from typing import Dict, Any, List, Generator


class MongoDBReceiver(ReadDatabaseReceiver, WriteDatabaseReceiver):
    """
    MongoDBReceiver is a class that handles the reception of data from a MongoDB database.
    It is designed to work with the MongoDB database system and provides methods to connect,
    query, and retrieve data from the database.
    """

    async def read_row(self) -> Dict[str, Any]:
        """Reads a single row."""
        pass

    async def read_bulk(self) -> List[Dict[str, Any]]:
        """Reads multiple rows as list."""
        pass

    async def read_bigdata(self) -> Generator[Dict[str, Any], None, None]:
        """Reads big data in a generator/streaming manner."""
        pass

    async def write_row(self, row: Dict[str, Any]):
        """Writes a single row."""
        pass

    async def write_bulk(self, data: List[Dict[str, Any]]):
        """Writes bulk data as list of rows."""
        pass

    async def write_bigdata(
        self, chunk_iterable: Generator[Dict[str, Any], None, None]
    ):
        """Writes data in chunks (e.g. generator)."""
        pass
