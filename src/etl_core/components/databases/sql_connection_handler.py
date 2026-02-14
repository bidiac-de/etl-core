from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, Optional, Tuple
from urllib.parse import quote_plus

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
        self._url: Optional[str] = None
        self._engine_kwargs: Dict[str, Any] = {}
        self._session_initializer: Optional[Callable[[Connection], None]] = None

    @staticmethod
    def build_url(
        *,
        dialect: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
    ) -> str:
        missing_required = any(
            [
                user in (None, ""),
                host in (None, ""),
                port is None,
                database in (None, ""),
            ]
        )
        if missing_required:
            raise ValueError(f"{dialect} requires user, host, port, and database.")
        safe_user = quote_plus(user)
        safe_password = quote_plus(password or "")
        return f"{dialect}://{safe_user}:{safe_password}@{host}:{port}/{database}"

    @staticmethod
    def _resolve_dialect(receiver: Any) -> str:
        dialect = getattr(receiver, "SQL_DIALECT", None)
        if dialect is None:
            dialect = getattr(getattr(receiver, "__class__", None), "SQL_DIALECT", None)
        if not dialect:
            raise ValueError(
                "Receiver must define SQL_DIALECT to resolve SQL connection driver."
            )
        return dialect

    def connect_with_credentials(
        self,
        *,
        credentials: Any,
        receiver: Any,
        engine_kwargs: Optional[Dict[str, Any]] = None,
        session_initializer: Optional[Callable[[Connection], None]] = None,
        eager: bool = True,
    ) -> Tuple[PoolKey, Optional[Engine]]:
        dialect = self._resolve_dialect(receiver)
        url = self.build_url(
            dialect=dialect,
            user=credentials.user,
            password=credentials.decrypted_password,
            host=credentials.host,
            port=credentials.port,
            database=credentials.database,
        )
        return self.connect(
            url=url,
            engine_kwargs=engine_kwargs,
            session_initializer=session_initializer,
            eager=eager,
        )

    def connect(
        self,
        *,
        url: str,
        engine_kwargs: Optional[Dict[str, Any]] = None,
        session_initializer: Optional[Callable[[Connection], None]] = None,
        eager: bool = True,
    ) -> Tuple[PoolKey, Optional[Engine]]:
        self._url = url
        self._engine_kwargs = dict(engine_kwargs or {})
        self._session_initializer = session_initializer
        self._key = PoolKey.for_sql(url=url, engine_kwargs=self._engine_kwargs)
        if eager:
            self._key, self._engine = self._registry.get_sql_engine(
                url=url, engine_kwargs=engine_kwargs
            )
        else:
            self._engine = None
        return self._key, self._engine

    def _ensure_engine(self) -> Engine:
        if self._engine is None:
            if not self._url:
                raise RuntimeError(
                    "SQLConnectionHandler.connect() must be called before lease()."
                )
            self._key, self._engine = self._registry.get_sql_engine(
                url=self._url, engine_kwargs=self._engine_kwargs or None
            )
        return self._engine

    @contextmanager
    def lease(
        self, *, initialize_session: bool = True
    ) -> Generator[Connection, None, None]:
        if not self._key or not self._engine:
            self._ensure_engine()
        self._registry.lease_sql(self._key)
        try:
            with self._engine.connect() as conn:
                if initialize_session and self._session_initializer:
                    self._session_initializer(conn)
                yield conn
        finally:
            self._registry.release_sql(self._key)

    def close_pool(self, *, force: bool = False) -> bool:
        if not self._key:
            return False
        return self._registry.close_pool(self._key, force=force)

    def stats(self) -> dict:
        return self._registry.stats()
