from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional, Tuple

from sqlalchemy.engine import Connection, Engine

from etl_core.components.databases.pool_registry import (
    ConnectionPoolRegistry,
    PoolKey,
)


class SQLConnectionHandler:
    """
    Family-level SQL handler (Postgres/MySQL/MariaDB/SQLite via SQLAlchemy).
    Keeps URL building outside; focuses on leasing from the registry.
    """

    def __init__(self) -> None:
        self._registry = ConnectionPoolRegistry.instance()
        self._key: Optional[PoolKey] = None
        self._engine: Optional[Engine] = None

    @staticmethod
    def build_url(
        *,
        comp_type: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
    ) -> str:
        # Map database types to SQLAlchemy driver prefixes
        driver_map = {
            "read_postgresql": "postgresql+psycopg2",
            "write_postgresql": "postgresql+psycopg2",
            "read_mariadb": "mysql+mysqlconnector",
            "write_mariadb": "mysql+mysqlconnector",
            "read_mysql": "mysql+mysqlconnector",
            "write_mysql": "mysql+mysqlconnector",
            "read_sqlite": "sqlite",
            "write_sqlite": "sqlite",
            "read_sqlserver": "mssql+pyodbc",
            "write_sqlserver": "mssql+pyodbc",
        }

        # Use the mapped driver or fall back to the original db_type
        driver = driver_map.get(comp_type, comp_type)

        if not all([user, password, host, port, database]):
            raise ValueError(
                f"{comp_type} requires user, password, host, port, and database."
            )
        return f"{driver}://{user}:{password}@{host}:{port}/{database}"

    def connect(
        self, *, url: str, engine_kwargs: Optional[Dict[str, Any]] = None
    ) -> Tuple[PoolKey, Engine]:
        self._key, self._engine = self._registry.get_sql_engine(
            url=url, engine_kwargs=engine_kwargs
        )
        return self._key, self._engine

    @contextmanager
    def lease(self) -> Generator[Connection, None, None]:
        if not self._key or not self._engine:
            raise RuntimeError(
                "SQLConnectionHandler.connect() must be called before lease()."
            )
        self._registry.lease_sql(self._key)
        try:
            with self._engine.connect() as conn:
                yield conn
        finally:
            self._registry.release_sql(self._key)

    def close_pool(self, *, force: bool = False) -> bool:
        if not self._key:
            return False
        return self._registry.close_pool(self._key, force=force)

    def stats(self) -> dict:
        return self._registry.stats()
