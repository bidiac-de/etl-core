from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, PrivateAttr, model_validator

from etl_core.components.databases.database import DatabaseComponent
from etl_core.components.databases.mongodb.mongodb_connection_handler import (
    MongoConnectionHandler,
)
from etl_core.components.databases.pool_args import build_mongo_client_kwargs


class MongoDBComponent(DatabaseComponent, ABC):
    """
    Base class for MongoDB components.

    Responsibilities:
    - resolve active credentials via CredentialsMappingContext (handled by parent)
    - build MongoDB URI and client kwargs from resolved credentials
    - connect via a pooled MongoConnectionHandler
    - expose 'entity_name' (collection) and 'database_name'
    """

    auth_db_name: Optional[str] = Field(
        default="admin",
        description=(
            "Authentication database name. If not set, defaults to the database "
            "specified in the resolved credentials."
        ),
    )

    # Lazily-initialized connection bits
    _connection_handler: Optional[MongoConnectionHandler] = PrivateAttr(default=None)
    _mongo_uri: Optional[str] = PrivateAttr(default=None)
    _database_name: Optional[str] = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _build_objects(self) -> "MongoDBComponent":
        """
        Align with SQL components: try to establish the connection during model
        validation so misconfiguration fails early. Safe to call multiple times.
        """
        self._setup_connection()
        return self

    @property
    def connection_handler(self) -> MongoConnectionHandler:
        """
        Access the pooled connection handler. Initializes on first access if needed.
        """
        if self._connection_handler is None:
            self._setup_connection()
        if self._connection_handler is None:
            raise RuntimeError(
                f"{self.name}: Mongo connection handler not initialized."
            )
        return self._connection_handler

    @property
    def database_name(self) -> str:
        """
        Expose the database name after credentials are known.
        """
        if not self._database_name:
            self._setup_connection()
        if not self._database_name:
            raise RuntimeError(f"{self.name}: database name not available.")
        return self._database_name

    def _setup_connection(self) -> None:
        """
        Idempotent connection setup using the already-resolved credentials from
        DatabaseComponent. Does nothing if handler is already present.
        """
        if self._connection_handler is not None:
            return

        creds_map = self._get_credentials()
        if self._credentials is None:
            ctx = self.get_resolved_context()
            if ctx is None:
                return
            self._credentials = ctx.resolve_active_credentials()

        self._database_name = creds_map["database"]

        handler = MongoConnectionHandler()
        uri = MongoConnectionHandler.build_uri(
            user=creds_map["user"],
            password=creds_map["password"],
            host=creds_map["host"],
            port=creds_map["port"],
            auth_db=self.auth_db_name or creds_map["database"],
            params=None,
        )
        self._mongo_uri = uri

        client_kwargs = build_mongo_client_kwargs(self._credentials)
        handler.connect(uri=uri, client_kwargs=client_kwargs)
        self._connection_handler = handler

    def __del__(self) -> None:
        # Best-effort pool cleanup
        if getattr(self, "_connection_handler", None):
            try:
                self._connection_handler.close_pool(force=True)
            except Exception:
                pass

    @abstractmethod
    async def process_row(self, *args: Any, **kwargs: Any) -> dict:
        raise NotImplementedError

    @abstractmethod
    async def process_bulk(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    async def process_bigdata(self, *args: Any, **kwargs: Any) -> dd.DataFrame:
        raise NotImplementedError
