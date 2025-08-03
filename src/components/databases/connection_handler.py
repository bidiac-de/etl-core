from typing import Any, Dict, Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection as SQLConnection
from pymongo import MongoClient
from pymongo.database import Database as MongoDatabase


class ConnectionHandler:
    """Handles connections for SQL and MongoDB databases."""

    _SQL_DIALECTS: Dict[str, str] = {
        "postgres": "postgresql+psycopg2",
        "mysql": "mysql+mysqlconnector",
        "mariadb": "mysql+mysqlconnector",
        "sqlite": "sqlite",
    }

    def __init__(self):
        self.connection: Optional[Any] = None

    @classmethod
    def create(
            cls,
            db_type: str,
            user: Optional[str] = None,
            password: Optional[str] = None,
            host: Optional[str] = None,
            port: Optional[int] = None,
            database: Optional[str] = None,
            **extras: Any
    ) -> "ConnectionHandler":
        db = db_type.lower()


        if db in cls._SQL_DIALECTS:
            driver = cls._SQL_DIALECTS[db]
            if driver == "sqlite":
                if not database:
                    raise ValueError("SQLite requires a database (file path).")
                url = f"sqlite:///{database}"
            else:
                if not all([user, password, host, port, database]):
                    raise ValueError(f"{db} requires user, password, host, port, and database.")
                url = f"{driver}://{user}:{'***'}@{host}:{port}/{database}"  # password masked

            engine = create_engine(url.replace(f":***@", ":***@"), **extras)
            conn = engine.connect()

            safe_url = url.replace(f":***@", ":***@")  # already masked
            print(f"[ConnectionHandler] Created SQLAlchemy connection: {safe_url}")

            instance = cls()
            instance.connection = conn
            return instance

        elif db == "mongodb":
            if not host or not database:
                raise ValueError("MongoDB requires at least host and database.")

            creds = f"{user}:***@" if user and password else ""
            port_part = f":{port}" if port else ""
            uri = f"mongodb://{creds}{host}{port_part}"

            client = MongoClient(uri.replace("***", "***"), **extras)
            conn = client[database]

            safe_uri = f"mongodb://{host}{port_part}/{database}"
            print(f"[ConnectionHandler] Created MongoDB connection: {safe_uri}")

            instance = cls()
            instance.connection = conn
            return instance

        else:
            raise ValueError(f"Unsupported db_type: {db_type!r}")

    def close(self):
        """Close the current connection if possible."""
        if self.connection:
            if isinstance(self.connection, SQLConnection):
                self.connection.close()
                print("[ConnectionHandler] Closed SQL connection.")
            elif isinstance(self.connection, MongoDatabase):
                self.connection.client.close()
                print("[ConnectionHandler] Closed MongoDB connection.")
            self.connection = None