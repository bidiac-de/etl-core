from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, model_validator, PrivateAttr

from etl_core.components.databases.database import DatabaseComponent
from etl_core.components.databases.mongodb.mongodb_connection_handler import (
    MongoConnectionHandler,
)
from etl_core.components.databases.pool_args import build_mongo_client_kwargs
from etl_core.receivers.databases.mongodb.mongodb_receiver import MongoDBReceiver


class MongoDBComponent(DatabaseComponent, ABC):
    """
    Base class for MongoDB components.

    Mirrors SQLDatabaseComponent responsibilities:
    - resolves credentials from context
    - builds a URI and client kwargs
    - connects via a family-specific connection handler
    - exposes 'entity_name' and 'database_name'
    """

    auth_db_name: str = Field(default=None,
        description=(
            "Authentication database name. If not set, defaults to the database "
            "specified in the credentials."
        ),
    )

    _connection_handler: MongoConnectionHandler
    _mongo_uri: str = PrivateAttr()
    _database_name: str = PrivateAttr()

    @model_validator(mode="after")
    def _build_objects(self) -> "MongoDBComponent":
        self._receiver = MongoDBReceiver()
        self._setup_connection()
        return self

    @property
    def connection_handler(self) -> MongoConnectionHandler:
        return self._connection_handler
    @property
    def database_name(self) -> str:
        return self._database_name

    def _setup_connection(self) -> None:
        if not self._context:
            return

        creds = self._get_credentials()

        self._database_name = creds["database"]

        self._connection_handler = MongoConnectionHandler()
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
        self._connection_handler.connect(uri=uri, client_kwargs=client_kwargs)

    def __del__(self) -> None:
        if hasattr(self, "_connection_handler") and self._connection_handler:
            self._connection_handler.close_pool(force=True)

    @abstractmethod
    async def process_row(self, *args: Any, **kwargs: Any) -> dict:
        raise NotImplementedError

    @abstractmethod
    async def process_bulk(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    async def process_bigdata(self, *args: Any, **kwargs: Any) -> dd.DataFrame:
        raise NotImplementedError
