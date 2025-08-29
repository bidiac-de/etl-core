"""
Fixtures for SQL Server component tests.

This file provides common fixtures used across SQL Server test files.
"""

import pytest
from unittest.mock import Mock

from etl_core.context.context import Context
from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials
from etl_core.context.context_parameter import ContextParameter
from etl_core.components.databases.sqlserver.sqlserver_read import SQLServerRead
from etl_core.components.databases.sqlserver.sqlserver_write import SQLServerWrite


@pytest.fixture
def test_creds():
    """Provide test credentials for testing."""
    return ("testuser", "testpassword123")


@pytest.fixture
def sample_credentials(test_creds):
    """Create sample credentials for testing."""
    user, password = test_creds
    return Credentials(
        credentials_id=1,
        name="test_db_creds",
        user=user,
        host="localhost",
        port=1433,  # SQL Server default port
        database="testdb",
        password=password,
        pool_max_size=10,
        pool_timeout_s=30,
    )


@pytest.fixture
def sample_context(sample_credentials):
    """Create sample context with credentials."""
    context = Context(
        id=1,
        name="test_context",
        environment=Environment.TEST,
        parameters={
            "db_host": ContextParameter(
                id=1, key="db_host", value="localhost", type="string", is_secure=False
            ),
            "db_port": ContextParameter(
                id=2, key="db_port", value="1433", type="string", is_secure=False
            ),
        },
    )
    context.add_credentials(sample_credentials)
    return context


@pytest.fixture
def mock_context(sample_context):
    """Create mock context for testing."""
    return sample_context


@pytest.fixture
def multiple_credentials(test_creds):
    """Create multiple credentials for testing."""
    user, password = test_creds
    creds1 = Credentials(
        credentials_id=1,
        name="first_db",
        user=user,
        host="localhost",
        port=1433,
        database="testdb",
        password=password,
        pool_max_size=10,
        pool_timeout_s=30,
    )
    creds2 = Credentials(
        credentials_id=2,
        name="second_db",
        user="user2",
        host="localhost",
        port=1433,
        database="seconddb",
        password="password2",
        pool_max_size=15,
        pool_timeout_s=45,
    )
    return [creds1, creds2]


@pytest.fixture
def context_with_multiple_credentials(multiple_credentials):
    """Create context with multiple credentials."""
    context = Context(
        id=2,
        name="multi_context",
        environment=Environment.TEST,
        parameters={},
    )
    for creds in multiple_credentials:
        context.add_credentials(creds)
    return context


@pytest.fixture
def sqlserver_read_component(sample_context):
    """Create SQL Server read component for testing."""
    return SQLServerRead(
        name="test_read",
        description="Test read component",
        comp_type="read_sqlserver",
        entity_name="users",
        query="SELECT * FROM users",
        credentials_id=1,
    )


@pytest.fixture
def sqlserver_write_component(sample_context):
    """Create SQL Server write component for testing."""
    return SQLServerWrite(
        name="test_write",
        description="Test write component",
        comp_type="write_sqlserver",
        entity_name="users",
        credentials_id=1,
    )


@pytest.fixture
def sample_sql_queries():
    """Provide sample SQL queries for testing."""
    return [
        "SELECT * FROM users",
        "SELECT id, name, email FROM users WHERE active = :active",
        "INSERT INTO users (name, email) VALUES (:name, :email)",
        "UPDATE users SET name = :name WHERE id = :id",
        "DELETE FROM users WHERE id = :id",
        """
        SELECT u.id, u.name, u.email, p.phone
        FROM users u
        LEFT JOIN profiles p ON u.id = p.user_id
        WHERE u.active = :active_status
        """,
        "SELECT COUNT(*) FROM users WHERE created_date >= :start_date",
        "SELECT DISTINCT department FROM employees WHERE salary > :min_salary",
    ]


@pytest.fixture
def sample_query_params():
    """Provide sample query parameters for testing."""
    return [
        {},
        {"active": 1},
        {"name": "John Doe", "email": "john@example.com"},
        {"name": "Jane Smith", "id": 123},
        {"id": 456},
        {"active_status": True},
        {"start_date": "2024-01-01"},
        {"min_salary": 50000},
        {"user_id": 789, "status": "active"},
        {"limit": 100, "offset": 0},
    ]


@pytest.fixture
def mock_connection_handler():
    """Create mock connection handler for testing."""
    handler = Mock()
    mock_connection = Mock()
    mock_connection.execute.return_value = Mock()
    mock_context = Mock()
    mock_context.__enter__ = Mock(return_value=mock_connection)
    mock_context.__exit__ = Mock(return_value=None)
    handler.lease.return_value = mock_context
    return handler


@pytest.fixture
def mock_metrics():
    """Create mock metrics for testing."""
    metrics = Mock()
    metrics.set_started = Mock()
    metrics.set_completed = Mock()
    metrics.set_failed = Mock()
    return metrics


@pytest.fixture
def sample_data():
    """Provide sample data for testing."""
    return [
        {"id": 1, "name": "John Doe", "email": "john@example.com", "active": True},
        {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "active": True},
        {"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "active": False},
        {"id": 4, "name": "Alice Brown", "email": "alice@example.com", "active": True},
    ]


@pytest.fixture
def sample_dataframe(sample_data):
    """Create sample pandas DataFrame for testing."""
    import pandas as pd
    return pd.DataFrame(sample_data)


@pytest.fixture
def sample_dask_dataframe(sample_dataframe):
    """Create sample Dask DataFrame for testing."""
    import dask.dataframe as dd
    return dd.from_pandas(sample_dataframe, npartitions=2)

