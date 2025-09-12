"""
Shared test fixtures for MariaDB database tests.
"""

from __future__ import annotations

import hashlib
import os
from typing import Dict, Tuple
from uuid import uuid4

import dask.dataframe as dd
import pandas as pd
import pytest
from unittest.mock import Mock, patch

from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials
from etl_core.persistance.handlers.credentials_handler import CredentialsHandler
from etl_core.persistance.handlers.context_handler import ContextHandler
from etl_core.components.databases.mariadb.mariadb_read import MariaDBRead
from etl_core.components.databases.mariadb.mariadb_write import MariaDBWrite


def derive_test_password(base_pw: str, purpose: str) -> str:
    """Deterministically derive a test password variant without hard-coded secrets."""
    digest = hashlib.blake2b(
        f"{purpose}:{base_pw}".encode("utf-8"), digest_size=6
    ).hexdigest()
    return f"{base_pw}_{digest}"


@pytest.fixture(autouse=True)
def _set_test_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Ensure env-based credential selection is deterministic across tests.
    """
    monkeypatch.setenv("COMP_ENV", Environment.TEST.value)
    monkeypatch.setenv("SECRET_BACKEND", "memory")


@pytest.fixture
def test_creds() -> Tuple[str, str]:
    # Provide these via your test env (e.g., in CI) or .env for local runs
    return os.environ["APP_TEST_USER"], os.environ["APP_TEST_PASSWORD"]


@pytest.fixture
def persisted_credentials(test_creds: Tuple[str, str]) -> Tuple[Credentials, str]:
    """
    Persist a Credentials object.
    Returns (Credentials model, credentials_id) where credentials_id is system-generated.
    """
    user, password = test_creds
    creds = Credentials(
        name="test_db_creds",
        user=user,
        host="localhost",
        port=3306,
        database="testdb",
        password=password,
        pool_max_size=10,
        pool_timeout_s=30,
    )
    credentials_id = CredentialsHandler().upsert(creds)
    return creds, credentials_id


@pytest.fixture
def persisted_mapping_context_id(persisted_credentials: Tuple[Credentials, str]) -> str:
    """
    Create/update a mapping context (env -> credentials_id) and return its context_id.
    """
    _, credentials_id = persisted_credentials
    context_id = str(uuid4())

    ContextHandler().upsert_credentials_mapping_context(
        context_id=context_id,
        name="test_mapping_ctx",
        environment=Environment.TEST.value,
        mapping_env_to_credentials_id={Environment.TEST.value: credentials_id},
    )
    return context_id


@pytest.fixture
def mock_connection_handler() -> Mock:
    """Create a mock connection handler for testing components safely."""
    handler = Mock()
    handler.lease.return_value.__enter__.return_value = Mock()
    handler.lease.return_value.__exit__.return_value = None
    return handler


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["John", "Jane", "Bob"],
            "email": ["john@test.com", "jane@test.com", "bob@test.com"],
        }
    )


@pytest.fixture
def sample_dask_dataframe() -> dd.DataFrame:
    """Sample Dask DataFrame for testing."""
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
    """Create mock component metrics."""
    metrics = Mock()
    metrics.set_started = Mock()
    metrics.set_completed = Mock()
    metrics.set_failed = Mock()
    return metrics


@pytest.fixture
def mariadb_read_component(persisted_mapping_context_id: str) -> MariaDBRead:
    """
    Construct MariaDBRead with a persisted mapping context and patch the
    SQLConnectionHandler during validation/initialization.
    """
    with patch(
        "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
    ):
        comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="read_mariadb",
            entity_name="users",
            query="SELECT * FROM users",
            context_id=persisted_mapping_context_id,
        )
        return comp


@pytest.fixture
def mariadb_write_component(persisted_mapping_context_id: str) -> MariaDBWrite:
    """
    Construct MariaDBWrite with a persisted mapping context and patch the
    SQLConnectionHandler during validation/initialization.
    """
    with patch(
        "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
    ):
        from etl_core.components.wiring.schema import Schema
        from etl_core.components.wiring.column_definition import DataType, FieldDef

        schema = Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )

        comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="write_mariadb",
            entity_name="users",
            in_port_schemas={"in": schema},
            context_id=persisted_mapping_context_id,
        )
        return comp


@pytest.fixture
def multiple_credentials(test_creds: Tuple[str, str]) -> Dict[str, Credentials]:
    """
    Create multiple Credentials models (not persisted unless a test needs it).
    """
    _, base_pw = test_creds

    return {
        "minimal": Credentials(
            name="minimal_creds",
            user="minuser",
            host="localhost",
            port=3306,
            database="mindb",
            password=base_pw,
        ),
        "with_pool": Credentials(
            name="pool_creds",
            user="pooluser",
            host="localhost",
            port=3306,
            database="pooldb",
            password=derive_test_password(base_pw, "pool"),
            pool_max_size=50,
            pool_timeout_s=60,
        ),
        "special_chars": Credentials(
            name="special_creds",
            user="user@domain",
            host="localhost",
            port=3306,
            database="test-db_123",
            password=derive_test_password(base_pw, "special"),
        ),
        "no_password": Credentials(
            name="no_pass_creds",
            user="nopassuser",
            host="localhost",
            port=3306,
            database="nopassdb",
        ),
    }
