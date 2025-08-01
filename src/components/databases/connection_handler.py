from typing import Optional
from src.context.context import Context


class ConnectionHandler:
    def __init__(self):
        self.pool: Optional[object] = None
        self.connection: Optional[object] = None

    def create_connection(self, context: Context):
        """Create a single database connection using context parameters."""
        user = context.get_parameter("user")
        password = context.get_parameter("password")
        database = context.get_parameter("database")
        host = context.get_parameter("host")

        #  DB-Verbindung?
        self.connection = f"Connection(user={user}, db={database}, host={host})"
        print(f"[ConnectionHandler] Created connection: {self.connection}")
        return self.connection

    def close_connection(self):
        """Close the single database connection."""
        if self.connection:
            print(f"[ConnectionHandler] Closing connection: {self.connection}")
            self.connection = None

    def create_connection_pool(self, context: Context, pool_size: int = 5):
        """Create a database connection pool."""
        self.pool = f"Pool(size={pool_size}, db={context.get_parameter('database')})"
        print(f"[ConnectionHandler] Created connection pool: {self.pool}")
        return self.pool

    def close_connection_pool(self):
        """Close the database connection pool."""
        if self.pool:
            print(f"[ConnectionHandler] Closing connection pool: {self.pool}")
            self.pool = None