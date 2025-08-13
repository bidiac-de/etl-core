from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional, Tuple

from pymongo.database import Database as MongoDatabase
from pymongo import MongoClient

from src.components.databases.pool_registry import ConnectionPoolRegistry, PoolKey


class MongoConnectionHandler:
    """
    MongoDB handler that reuses clients via the shared registry.
    """

    def __init__(self) -> None:
        self._registry = ConnectionPoolRegistry.instance()
        self._key: Optional[PoolKey] = None
        self._client: Optional[MongoClient] = None
        self._db: Optional[MongoDatabase] = None

    @staticmethod
    def build_uri(
        *,
        host: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[int] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> str:
        creds = f"{user}:{password}@" if user and password else ""
        port_part = f":{port}" if port else ""
        base = f"mongodb://{creds}{host}{port_part}"
        if not params:
            return base
        # naive param join; insert your own encoding as needed
        kv = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        return f"{base}/?{kv}"

    def connect(
        self, *, uri: str, database: str, client_kwargs: Optional[Dict[str, Any]] = None
    ) -> Tuple[PoolKey, MongoClient]:
        self._key, self._client = self._registry.get_mongo_client(
            uri=uri, client_kwargs=client_kwargs
        )
        self._db = self._client[database]
        return self._key, self._client

    @contextmanager
    def lease(self) -> Generator[MongoDatabase, None, None]:
        if not self._key or not self._db:
            raise RuntimeError(
                "MongoConnectionHandler.connect() must be called before lease()."
            )
        self._registry.lease_mongo(self._key)
        try:
            yield self._db
        finally:
            self._registry.release_mongo(self._key)

    def close_pool(self, *, force: bool = False) -> bool:
        if not self._key:
            return False
        return self._registry.close_pool(self._key, force=force)

    def stats(self) -> dict:
        return self._registry.stats()
