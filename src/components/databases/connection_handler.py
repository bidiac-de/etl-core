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
                real_url = f"sqlite:///{database}"
                masked_url = real_url
            else:

                if not all([host, port, database]):
                    raise ValueError(f"{db} requires host, port, and database.")
                
                # Credentials can be provided later
                if not user or not password:
                    # Create connection without authentication for now
                    real_url = f"{driver}://{host}:{port}/{database}"
                    masked_url = f"{driver}://{host}:{port}/{database}"
                else:
                    real_url = f"{driver}://{user}:{password}@{host}:{port}/{database}"
                    masked_url = f"{driver}://{user}:***@{host}:{port}/{database}"


                real_url = f"{driver}://{user}:{password}@{host}:{port}/{database}"
                masked_url = f"{driver}://{user}:***@{host}:{port}/{database}"

            engine = create_engine(real_url, **extras)
            conn = engine.connect()
            print(f"[ConnectionHandler] Created SQLAlchemy connection: {masked_url}")

            instance = cls()
            instance.connection = conn
            return instance

        elif db == "mongodb":
            if not host or not database:
                raise ValueError("MongoDB requires at least host and database.")

            creds = f"{user}:{password}@" if user and password else ""
            masked_creds = f"{user}:***@" if user and password else ""
            port_part = f":{port}" if port else ""

            real_uri = f"mongodb://{creds}{host}{port_part}"
            masked_uri = f"mongodb://{masked_creds}{host}{port_part}"

            client = MongoClient(real_uri, **extras)
            conn = client[database]

            print(f"[ConnectionHandler] Created MongoDB connection: {masked_uri}/{database}")

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