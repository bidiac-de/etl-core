from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, PrivateAttr

from etl_core.components.databases.database import DatabaseComponent
from etl_core.components.databases.mongodb.mongodb_connection_handler import (
    MongoConnectionHandler,
)
from etl_core.components.databases.pool_args import build_mongo_client_kwargs


class MongoDBComponent(DatabaseComponent, ABC):
    """
    Base class for MongoDB components.

    Responsibilities:
    - resolve credentials from context
    - build a URI and client kwargs
    - connect via a MongoConnectionHandler
    - expose 'entity_name' and 'database_name'
    """

    auth_db_name: Optional[str] = Field(
        default=None,
        description=(
            "Authentication database name. If not set, defaults to the database "
            "specified in the credentials."
        ),
    )

    # Lazily-initialized connection bits
    _connection_handler: Optional[MongoConnectionHandler] = PrivateAttr(default=None)
    _mongo_uri: Optional[str] = PrivateAttr(default=None)
    _database_name: Optional[str] = PrivateAttr(default=None)

    @property
    def connection_handler(self) -> MongoConnectionHandler:
        """
        Lazily initialize the connection when first accessed, so tests (and user code)
        can assign `context` after construction without tripping a None handler.
        """
        if self._connection_handler is None:
            self._setup_connection()
            if self._connection_handler is None:
                raise RuntimeError(
                    "Mongo connection is not initialized; context may be missing."
                )
        return self._connection_handler

    @property
    def database_name(self) -> str:
        """
        Ensure database name is available once context/credentials are known.
        """
        if not self._database_name:
            # Build once if a caller asks for it early
            self._setup_connection()
        if not self._database_name:
            raise RuntimeError(
                "Database name is not resolved; make sure context/credentials are set."
            )
        return self._database_name

    def _setup_connection(self) -> None:
        """
        Idempotent: builds and connects the Mongo handler once `context` is present.
        Safe to call multiple times; subsequent calls return early.
        """
        if self._connection_handler is not None:
            return
        if not getattr(self, "_context", None):
            # Context not set yet; defer initialization.
            return

        creds = self._get_credentials()
        self._database_name = creds["database"]

        handler = MongoConnectionHandler()
        uri = MongoConnectionHandler.build_uri(
            user=creds["user"],
            password=creds["password"],
            host=creds["host"],
            port=creds["port"],
            auth_db=self.auth_db_name,
            params=None,
        )
        self._mongo_uri = uri

        credentials_obj = self._context.get_credentials(self.credentials_id)
        client_kwargs = build_mongo_client_kwargs(credentials_obj)
        handler.connect(uri=uri, client_kwargs=client_kwargs)
        self._connection_handler = handler

    def __del__(self) -> None:
        # Best-effort pool cleanup
        if getattr(self, "_connection_handler", None):
            try:
                self._connection_handler.close_pool(force=True)  # type: ignore[union-attr]
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
