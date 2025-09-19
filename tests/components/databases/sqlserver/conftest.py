"""
Shared fixtures for SQL Server database tests (mapping-context based).

Pattern (matches MariaDB/PostgreSQL tests):
- Persist Credentials (provider_id == credentials_id)
- Create a mapping Context (env -> credentials_id) via ContextHandler
- Build components with context_id (not credentials_id)
"""

from __future__ import annotations

import os
from typing import Dict, Tuple
from uuid import uuid4
from unittest.mock import Mock, patch

import dask.dataframe as dd
import pandas as pd
import pytest
import contextlib

from etl_core.components.databases.sqlserver.sqlserver import SQLServerComponent
from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials
from etl_core.components.databases.sqlserver.sqlserver_read import SQLServerRead
from etl_core.components.databases.sqlserver.sqlserver_write import SQLServerWrite
from etl_core.components.wiring.column_definition import DataType, FieldDef
from etl_core.components.wiring.schema import Schema
from etl_core.singletons import (
    credentials_handler as _crh_singleton,
    context_handler as _ch_singleton,
)

SQL_CONNECTION_HANDLER_PATH = (
    "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
)


@pytest.fixture(autouse=True)
def _set_test_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force deterministic env + in-memory secrets during tests."""
    monkeypatch.setenv("EXECUTION_ENV", Environment.TEST.value)
    monkeypatch.setenv("SECRET_BACKEND", "memory")


@pytest.fixture(autouse=True)
def _patch_sqls_session_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    def _noop(self) -> None:
        return None

    monkeypatch.setattr(
        SQLServerComponent,
        "_setup_session_variables",
        _noop,
        raising=True,
    )

@pytest.fixture(autouse=True)
def _stub_sql_connection_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Autouse fixture to stub out SQLConnectionHandler globally.

    Prevents any real engine/ODBC connection attempts during tests.
    Provides a no-op lease() context manager and close_pool().
    """

    class _StubHandler:
        def __init__(self, *args, **kwargs):
            pass

        @contextlib.contextmanager
        def lease(self, *args, **kwargs):
            # Provide a dummy connection object with execute/commit methods
            class _Conn:
                def execute(self, *_a, **_kw): return None
                def commit(self): return None
            yield _Conn()

        def close_pool(self) -> bool:
            return True

    monkeypatch.setattr(
        SQL_CONNECTION_HANDLER_PATH,
        _StubHandler,
        raising=True,
    )

@pytest.fixture
def test_creds() -> Tuple[str, str]:
    # Provide these via env for local runs
    return os.environ["APP_TEST_USER"], os.environ["APP_TEST_PASSWORD"]


@pytest.fixture
def persisted_credentials(test_creds: Tuple[str, str]) -> Tuple[Credentials, str]:
    """
    Persist a Credentials object. Use the model's credentials_id as provider_id
    so the context <-> credentials mapping FK is satisfied.
    """
    user, password = test_creds
    creds = Credentials(
        name="mssql_test_creds",
        user=user,
        host="localhost",
        port=1433,
        database="testdb",
        password=password,
        pool_max_size=10,
        pool_timeout_s=30,
    )
    credentials_id = _crh_singleton().upsert(creds)
    return creds, credentials_id


@pytest.fixture
def persisted_mapping_context_id(persisted_credentials: Tuple[Credentials, str]) -> str:
    """
    Create/update a mapping context (env -> credentials_id) and return its provider_id.
    """
    _, creds_id = persisted_credentials
    context_id = str(uuid4())
    _ch_singleton().upsert_credentials_mapping_context(
        context_id=context_id,
        name="mssql_test_mapping_ctx",
        environment=Environment.TEST.value,
        mapping_env_to_credentials_id={Environment.TEST.value: creds_id},
    )
    return context_id


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["John", "Jane", "Bob"],
            "email": ["john@test.com", "jane@test.com", "bob@test.com"],
        }
    )


@pytest.fixture
def sample_dask_dataframe() -> dd.DataFrame:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": ["John", "Jane", "Bob", "Alice"],
            "email": [
                "john@test.com",
                "jane@test.com",
                "bob@test.com",
                "alice@test.com",
            ],
        }
    )
    return dd.from_pandas(df, npartitions=2)


@pytest.fixture
def mock_metrics() -> Mock:
    metrics = Mock()
    metrics.set_started = Mock()
    metrics.set_completed = Mock()
    metrics.set_failed = Mock()
    return metrics


@pytest.fixture
def sqlserver_read_component(persisted_mapping_context_id: str) -> SQLServerRead:
    """Construct SQLServerRead with a persisted mapping context; patch connection."""
    with patch(
        SQL_CONNECTION_HANDLER_PATH
    ):
        return SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            context_id=persisted_mapping_context_id,
        )


@pytest.fixture
def sqlserver_write_component(persisted_mapping_context_id: str) -> SQLServerWrite:
    """Construct SQLServerWrite with a persisted mapping context; patch connection."""
    with patch(
        SQL_CONNECTION_HANDLER_PATH
    ):
        schema = Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )
        return SQLServerWrite(
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver",
            entity_name="users",
            in_port_schemas={"in": schema},
            context_id=persisted_mapping_context_id,
        )


@pytest.fixture
def multiple_credentials(test_creds: Tuple[str, str]) -> Dict[str, Credentials]:
    """Build (unpersisted) credential variants for targeted tests."""
    _, base_pw = test_creds
    return {
        "minimal": Credentials(
            name="mssql_minimal",
            user="minuser",
            host="localhost",
            port=1433,
            database="mindb",
            password=base_pw,
        ),
        "with_pool": Credentials(
            name="mssql_pool",
            user="pooluser",
            host="localhost",
            port=1433,
            database="pooldb",
            password=base_pw,
            pool_max_size=50,
            pool_timeout_s=60,
        ),
        "no_password": Credentials(
            name="mssql_nopass",
            user="nopassuser",
            host="localhost",
            port=1433,
            database="nopassdb",
        ),
    }
