from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional, Tuple
from urllib.parse import quote_plus

from etl_core.components.databases.pool_registry import (
    ConnectionPoolRegistry,
    PoolKey,
)


class MongoConnectionHandler:
    """
    MongoDB handler returning pooled AsyncIOMotorClient instances.
    """

    def __init__(self) -> None:
        self._registry = ConnectionPoolRegistry.instance()
        self._key: Optional[PoolKey] = None
        self._client: Optional[Any] = None  # AsyncIOMotorClient

    @staticmethod
    def build_uri(
        *,
        host: str,
        port: int,
        user: Optional[str] = None,
        password: Optional[str] = None,
        auth_db: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Build a MongoDB connection URI.

        Rules:
        - If no user is provided (None/""), omit credentials entirely.
        - If user is provided but password is None/empty, include only the username.
        - Apply URL quoting to user/password.
        - host may be a comma-separated list for replica sets.
        """
        if user:
            u = quote_plus(user)
            if password:
                p = quote_plus(password)
                auth = f"{u}:{p}@"
            else:
                auth = f"{u}@"
        else:
            auth = ""

        base = f"mongodb://{auth}{host}:{port}"
        query: Dict[str, Any] = {}
        if auth_db:
            query["authSource"] = auth_db
        if params:
            query.update(params)

        if not query:
            return base

        kv = "&".join(f"{k}={v}" for k, v in query.items())
        return f"{base}/?{kv}"

    def connect(
        self, *, uri: str, client_kwargs: Optional[Dict[str, Any]] = None
    ) -> Tuple[PoolKey, Any]:
        """
        Create or fetch a pooled AsyncIOMotorClient for the given URI.
        Returns the pool key and the client.
        """
        self._key, self._client = self._registry.get_mongo_client(
            uri=uri, client_kwargs=client_kwargs or {}
        )
        return self._key, self._client

    @contextmanager
    def lease_collection(
        self, *, database: str, collection: str
    ) -> Generator[Tuple[Any, Any], None, None]:
        """
        Context-manage a lease on the pooled client and yield (client, collection).
        """
        if not self._key or not self._client:
            raise RuntimeError(
                "MongoConnectionHandler.connect() must be "
                "called before lease_collection()."
            )
        self._registry.lease_mongo(self._key)
        try:
            db = self._client[database]
            coll = db[collection]
            yield self._client, coll
        finally:
            self._registry.release_mongo(self._key)

    def close_pool(self, *, force: bool = False) -> bool:
        """Close the pool for this handlers key."""
        if not self._key:
            return False
        return self._registry.close_pool(self._key, force=force)

    def stats(self) -> dict:
        """Return registry stats"""
        return self._registry.stats()
