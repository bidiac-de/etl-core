from __future__ import annotations

import logging
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
        description=("Authentication database name"),
    )

    # Lazily-initialized connection bits
    _connection_handler: Optional[MongoConnectionHandler] = PrivateAttr(default=None)
    _mongo_uri: Optional[str] = PrivateAttr(default=None)
    _database_name: Optional[str] = PrivateAttr(default=None)
    _log: logging.Logger = PrivateAttr(
        default_factory=lambda: logging.getLogger(
            "etl_core.components.databases.mongodb"
        )
    )

    @model_validator(mode="after")
    def _build_objects(self) -> "MongoDBComponent":
        """
        Align with SQL components: try to establish the connection during model
        validation so misconfiguration fails early. Safe to call multiple times.
        """
        self._log.debug("%s: running MongoDB component validation", self.name)
        self._setup_connection()
        return self

    @property
    def connection_handler(self) -> MongoConnectionHandler:
        """
        Access the pooled connection handler. Initializes on first access if needed.
        """
        if self._connection_handler is None:
            self._log.debug(
                "%s: connection handler missing, triggering setup.", self.name
            )
            self._setup_connection()
        if self._connection_handler is None:
            self._log.error(
                "%s: Mongo connection handler still uninitialized after setup.",
                self.name,
            )
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
            self._log.debug("%s: database name missing, triggering setup.", self.name)
            self._setup_connection()
        if not self._database_name:
            self._log.error(
                "%s: database name still unavailable after setup.", self.name
            )
            raise RuntimeError(f"{self.name}: database name not available.")
        return self._database_name

    def _setup_connection(self) -> None:
        """
        Idempotent connection setup using the already-resolved credentials from
        DatabaseComponent. Does nothing if handler is already present.
        """
        if self._connection_handler is not None:
            self._log.debug("%s: reusing existing Mongo connection handler.", self.name)
            return

        creds_map = self._get_credentials()
        if self._credentials is None:
            ctx = self.get_resolved_context()
            if ctx is None:
                self._log.debug(
                    "%s: credentials context not resolved yet; deferring setup.",
                    self.name,
                )
                return
            self._credentials = ctx.resolve_active_credentials()

        self._database_name = creds_map["database"]
        self._log.debug(
            "%s: resolved Mongo database '%s' from credentials.",
            self.name,
            self._database_name,
        )

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

        self._log.debug(
            "%s: building Mongo client for host=%s port=%s auth_db=%s.",
            self.name,
            creds_map["host"],
            creds_map["port"],
            self.auth_db_name or creds_map["database"],
        )

        client_kwargs = build_mongo_client_kwargs(self._credentials)
        try:
            handler.connect(uri=uri, client_kwargs=client_kwargs)
        except Exception:
            self._log.exception(
                "%s: failed to establish Mongo connection to %s:%s.",
                self.name,
                creds_map["host"],
                creds_map["port"],
            )
            raise
        self._log.info(
            "%s: Mongo connection established for database '%s' on host %s.",
            self.name,
            self._database_name,
            creds_map["host"],
        )
        self._connection_handler = handler

    def cleanup_after_execution(self, force: bool = False) -> None:
        handler = self._connection_handler
        if handler is None:
            return

        try:
            closed = handler.close_pool(force=force)
            if not closed and not force:
                self._log.debug(
                    "%s: Mongo pool still leased; forcing close.", self.name
                )
                closed = handler.close_pool(force=True)
        except Exception:
            self._log.exception(
                "%s: error while closing Mongo connection pool.", self.name
            )
            closed = False

        if closed:
            self._log.debug(
                "%s: Mongo connection pool closed after execution.", self.name
            )
        else:
            self._log.debug(
                "%s: Mongo connection pool not closed; leaving handler in place.",
                self.name,
            )

        if closed:
            self._connection_handler = None
            self._mongo_uri = None
            self._database_name = None

    def __del__(self) -> None:
        # Best-effort pool cleanup
        if getattr(self, "_connection_handler", None):
            try:
                self._log.debug("%s: closing Mongo connection pool.", self.name)
                self.cleanup_after_execution(force=True)
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
