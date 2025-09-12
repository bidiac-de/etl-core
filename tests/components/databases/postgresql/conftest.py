"""
Shared test fixtures for PostgreSQL database tests (mapping-context based).

This mirrors the MariaDB test style:
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

from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials
from etl_core.persistance.handlers.credentials_handler import CredentialsHandler
from etl_core.persistance.handlers.context_handler import ContextHandler
from etl_core.components.databases.postgresql.postgresql_read import PostgreSQLRead
from etl_core.components.databases.postgresql.postgresql_write import PostgreSQLWrite
from etl_core.components.wiring.column_definition import DataType, FieldDef
from etl_core.components.wiring.schema import Schema


@pytest.fixture(autouse=True)
def _set_test_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure deterministic env selection & in-memory secret store for tests."""
    monkeypatch.setenv("COMP_ENV", Environment.TEST.value)
    monkeypatch.setenv("SECRET_BACKEND", "memory")


@pytest.fixture
def test_creds() -> Tuple[str, str]:
    # Provide these via your test env or .env for local runs
    return os.environ["APP_TEST_USER"], os.environ["APP_TEST_PASSWORD"]


@pytest.fixture
def persisted_credentials(test_creds: Tuple[str, str]) -> Tuple[Credentials, str]:
    """
    Persist a Credentials object. IMPORTANT: use the model's credentials_id
    as the provider_id so mapping can reference it directly.
    """
    user, password = test_creds
    creds = Credentials(
        name="pg_test_creds",
        user=user,
        host="localhost",
        port=5432,
        database="testdb",
        password=password,
        pool_max_size=10,
        pool_timeout_s=30,
    )
    credentials_id = CredentialsHandler().upsert(creds)
    return creds, credentials_id


@pytest.fixture
def persisted_mapping_context_id(
    persisted_credentials: Tuple[Credentials, str]
) -> str:
    """
    Create/update a mapping context (env -> credentials_id) and return its provider_id.
    NOTE: Use upsert_credentials_mapping_context (no `context=` kw).
    """
    _, creds_id = persisted_credentials
    context_id = str(uuid4())
    ContextHandler().upsert_credentials_mapping_context(
        context_id=context_id,
        name="pg_test_mapping_ctx",
        environment=Environment.TEST.value,
        mapping_env_to_credentials_id={
            Environment.TEST.value: creds_id
        },
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
def postgresql_read_component(persisted_mapping_context_id: str) -> PostgreSQLRead:
    """
    Construct PostgreSQLRead with a persisted mapping context and patch the
    SQLConnectionHandler during validation/initialization.
    """
    with patch(
        "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
    ):
        comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query="SELECT * FROM users",
            context_id=persisted_mapping_context_id,
        )
        return comp


@pytest.fixture
def postgresql_write_component(persisted_mapping_context_id: str) -> PostgreSQLWrite:
    """
    Construct PostgreSQLWrite with a persisted mapping context and patch the
    SQLConnectionHandler during validation/initialization.
    """
    with patch(
        "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
    ):
        schema = Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )
        comp = PostgreSQLWrite(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            in_port_schemas={"in": schema},
            context_id=persisted_mapping_context_id,
        )
        return comp


@pytest.fixture
def multiple_credentials(test_creds: Tuple[str, str]) -> Dict[str, Credentials]:
    """Create multiple Credentials models; not persisted unless a test needs it."""
    _, base_pw = test_creds
    return {
        "minimal": Credentials(
            name="pg_minimal_creds",
            user="minuser",
            host="localhost",
            port=5432,
            database="mindb",
            password=base_pw,
        ),
        "with_pool": Credentials(
            name="pg_pool_creds",
            user="pooluser",
            host="localhost",
            port=5432,
            database="pooldb",
            password=base_pw,
            pool_max_size=50,
            pool_timeout_s=60,
        ),
        "no_password": Credentials(
            name="pg_no_pass_creds",
            user="nopassuser",
            host="localhost",
            port=5432,
            database="nopassdb",
        ),
    }
