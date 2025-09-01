"""
Shared test fixtures for MariaDB database tests.

This file provides common test fixtures that can be used across all MariaDB tests.
"""

from __future__ import annotations

import hashlib
import os
from typing import Tuple, Dict

import pytest
import pandas as pd
import dask.dataframe as dd
from unittest.mock import Mock, patch

from etl_core.context.credentials import Credentials
from etl_core.context.context import Context
from etl_core.context.environment import Environment
from etl_core.context.context_parameter import ContextParameter
from etl_core.components.databases.mariadb.mariadb_read import MariaDBRead
from etl_core.components.databases.mariadb.mariadb_write import MariaDBWrite


def derive_test_password(base_pw: str, purpose: str) -> str:
    """
    Deterministically derive a test password variant without hard-coded secrets.
    Keeps static analysis happy while remaining stable across a run.
    """
    digest = hashlib.blake2b(
        f"{purpose}:{base_pw}".encode("utf-8"), digest_size=6
    ).hexdigest()
    return f"{base_pw}_{digest}"


@pytest.fixture
def sample_credentials(test_creds: Tuple[str, str]) -> Credentials:
    """Create a real Credentials object for testing (password from env)."""
    user, password = test_creds
    return Credentials(
        credentials_id=1,
        name="test_db_creds",
        user=user,
        host="localhost",
        port=3306,
        database="testdb",
        password=password,
        pool_max_size=10,
        pool_timeout_s=30,
    )


@pytest.fixture
def sample_context(sample_credentials: Credentials) -> Context:
    """Create a real Context object with credentials."""
    context = Context(
        id=1,
        name="test_context",
        environment=Environment.TEST,
        parameters={
            "db_host": ContextParameter(
                id=1, key="db_host", value="localhost", type="string", is_secure=False
            ),
            "db_port": ContextParameter(
                id=2, key="db_port", value="3306", type="string", is_secure=False
            ),
        },
    )
    context.add_credentials(sample_credentials)
    return context


@pytest.fixture
def mock_connection_handler() -> Mock:
    """Create a mock connection handler for testing."""
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
def mariadb_read_component(sample_context: Context) -> MariaDBRead:
    """Create a MariaDBRead component with context."""
    with patch(
        "etl_core.components.databases.sql_connection_handler." "SQLConnectionHandler"
    ):
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="read_mariadb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = sample_context
        return read_comp


@pytest.fixture
def mariadb_write_component(sample_context: Context) -> MariaDBWrite:
    """Create a MariaDBWrite component with context."""
    with patch(
        "etl_core.components.databases.sql_connection_handler." "SQLConnectionHandler"
    ):
        write_comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="write_mariadb",
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = sample_context
        return write_comp


@pytest.fixture
def multiple_credentials(test_creds: Tuple[str, str]) -> Dict[str, Credentials]:
    """
    Create multiple credentials for testing different scenarios (env for passwords).
    """
    _, base_pw = test_creds

    return {
        "minimal": Credentials(
            credentials_id=2,
            name="minimal_creds",
            user="minuser",
            host="localhost",
            port=3306,
            database="mindb",
            password=base_pw,
        ),
        "with_pool": Credentials(
            credentials_id=3,
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
            credentials_id=4,
            name="special_creds",
            user="user@domain",
            host="localhost",
            port=3306,
            database="test-db_123",
            password=derive_test_password(base_pw, "special"),
        ),
        "no_password": Credentials(
            credentials_id=5,
            name="no_pass_creds",
            user="nopassuser",
            host="localhost",
            port=3306,
            database="nopassdb",
        ),
    }


@pytest.fixture
def context_with_multiple_credentials(
    multiple_credentials: Dict[str, Credentials],
) -> Context:
    """Create a context with multiple credentials."""
    context = Context(
        id=2,
        name="multi_creds_context",
        environment=Environment.TEST,
        parameters={},
    )
    for creds in multiple_credentials.values():
        context.add_credentials(creds)
    return context


@pytest.fixture
def sample_sql_queries() -> Dict[str, str]:
    """Sample SQL queries for testing."""
    return {
        "simple_select": "SELECT * FROM users",
        "parameterized": (
            "SELECT * FROM users WHERE id = %(id)s AND active = %(active)s"
        ),
        "complex_join": (
            "SELECT u.id, u.name, p.title "
            "FROM users u JOIN posts p ON u.id = p.user_id "
            "WHERE u.active = %(active)s"
        ),
        "aggregation": "SELECT COUNT(*) as count, active FROM users GROUP BY active",
        "insert": "INSERT INTO users (name, email) VALUES (%(name)s, %(email)s)",
        "update": "UPDATE users SET active = %(active)s WHERE id = %(id)s",
        "delete": "DELETE FROM users WHERE id = %(id)s",
    }


@pytest.fixture
def sample_query_params() -> Dict[str, object]:
    """Sample query parameters for testing."""
    return {
        "simple": {},
        "user_lookup": {"id": 1, "active": True},
        "bulk_insert": [
            {"name": "John", "email": "john@test.com"},
            {"name": "Jane", "email": "jane@test.com"},
            {"name": "Bob", "email": "bob@test.com"},
        ],
        "filter": {"active": True, "role": "admin"},
        "pagination": {"limit": 10, "offset": 0},
    }


@pytest.fixture
def mock_credentials(test_creds: Tuple[str, str]) -> Credentials:
    """Create a mock Credentials object for testing (env for password)."""
    user, password = test_creds
    return Credentials(
        credentials_id=1,
        name="test_creds",
        user=user,
        host="localhost",
        port=3306,
        database="testdb",
        password=password,
    )


@pytest.fixture
def mock_credentials_no_password() -> Credentials:
    """Create a mock Credentials object without password for testing."""
    return Credentials(
        credentials_id=2,
        name="test_creds_no_pass",
        user="testuser",
        host="localhost",
        port=3306,
        database="testdb",
    )


@pytest.fixture
def mock_context(test_creds: Tuple[str, str]) -> Mock:
    """Create a mock context with credentials (password from env)."""
    _user, _pwd = test_creds
    context = Mock()
    mock_credentials_obj = Mock()
    mock_credentials_obj.get_parameter.side_effect = lambda param: {
        "user": os.environ["APP_TEST_USER"],
        "password": os.environ["APP_TEST_PASSWORD"],
        "database": "testdb",
    }.get(param)
    mock_credentials_obj.decrypted_password = os.environ["APP_TEST_PASSWORD"]
    context.get_credentials.return_value = mock_credentials_obj
    return context
