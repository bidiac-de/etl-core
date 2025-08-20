"""
Shared test fixtures for MariaDB database tests.

This file provides common test fixtures that can be used across all MariaDB tests.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch

from src.etl_core.context.credentials import Credentials
from src.etl_core.context.context import Context
from src.etl_core.context.environment import Environment
from src.etl_core.context.context_parameter import ContextParameter
from src.etl_core.components.databases.mariadb.mariadb_read import MariaDBRead
from src.etl_core.components.databases.mariadb.mariadb_write import MariaDBWrite


@pytest.fixture
def sample_credentials():
    """Create a real Credentials object for testing."""
    return Credentials(
        credentials_id=1,
        name="test_db_creds",
        user="testuser",
        database="testdb",
        password="testpass123",
        pool_max_size=10,
        pool_timeout_s=30,
    )


@pytest.fixture
def sample_context(sample_credentials):
    """Create a real Context object with credentials."""
    context = Context(
        id=1,
        name="test_context",
        environment=Environment.TEST,
        parameters={
            "db_host": ContextParameter(
                id=1,
                key="db_host",
                value="localhost",
                type="string",
                is_secure=False,
            ),
            "db_port": ContextParameter(
                id=2, key="db_port", value="3306", type="string", is_secure=False
            ),
        },
    )
    # Add credentials to context
    context.add_credentials(sample_credentials)
    return context


@pytest.fixture
def mock_connection_handler():
    """Create a mock connection handler for testing."""
    handler = Mock()
    handler.lease.return_value.__enter__.return_value = Mock()
    handler.lease.return_value.__exit__.return_value = None
    return handler


@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["John", "Jane", "Bob"],
        "email": ["john@test.com", "jane@test.com", "bob@test.com"]
    })


@pytest.fixture
def sample_dask_dataframe():
    """Sample Dask DataFrame for testing."""
    try:
        import dask.dataframe as dd
        df = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "name": ["John", "Jane", "Bob", "Alice"],
            "email": ["john@test.com", "jane@test.com", "bob@test.com", "alice@test.com"]
        })
        return dd.from_pandas(df, npartitions=2)
    except ImportError:
        # Return a mock if dask is not available
        mock_ddf = Mock()
        mock_ddf.npartitions = 2
        mock_ddf.map_partitions.return_value = mock_ddf
        mock_ddf.compute.return_value = mock_ddf
        return mock_ddf


@pytest.fixture
def mock_metrics():
    """Create mock component metrics."""
    metrics = Mock()
    metrics.set_started = Mock()
    metrics.set_completed = Mock()
    metrics.set_failed = Mock()
    return metrics


@pytest.fixture
def mariadb_read_component(sample_context):
    """Create a MariaDBRead component with context."""
    with patch('src.etl_core.components.databases.sql_connection_handler.SQLConnectionHandler'):
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            host="localhost",
            port=3306,
            credentials_id=1,
        )
        read_comp.context = sample_context
        return read_comp


@pytest.fixture
def mariadb_write_component(sample_context):
    """Create a MariaDBWrite component with context."""
    with patch('src.etl_core.components.databases.sql_connection_handler.SQLConnectionHandler'):
        write_comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="database",
            database="testdb",
            entity_name="users",
            host="localhost",
            port=3306,
            credentials_id=1,
        )
        write_comp.context = sample_context
        return write_comp


@pytest.fixture
def multiple_credentials():
    """Create multiple credentials for testing different scenarios."""
    return {
        "minimal": Credentials(
            credentials_id=2,
            name="minimal_creds",
            user="minuser",
            database="mindb",
            password="minpass",
        ),
        "with_pool": Credentials(
            credentials_id=3,
            name="pool_creds",
            user="pooluser",
            database="pooldb",
            password="poolpass",
            pool_max_size=50,
            pool_timeout_s=60,
        ),
        "special_chars": Credentials(
            credentials_id=4,
            name="special_creds",
            user="user@domain",
            database="test-db_123",
            password="pass@word#123",
        ),
        "no_password": Credentials(
            credentials_id=5,
            name="no_pass_creds",
            user="nopassuser",
            database="nopassdb",
        ),
    }


@pytest.fixture
def context_with_multiple_credentials(multiple_credentials):
    """Create a context with multiple credentials."""
    context = Context(
        id=2,
        name="multi_creds_context",
        environment=Environment.TEST,
        parameters={}
    )
    
    for creds in multiple_credentials.values():
        context.add_credentials(creds)
    
    return context


@pytest.fixture
def sample_sql_queries():
    """Sample SQL queries for testing."""
    return {
        "simple_select": "SELECT * FROM users",
        "parameterized": "SELECT * FROM users WHERE id = %(id)s AND active = %(active)s",
        "complex_join": """
            SELECT u.id, u.name, p.title 
            FROM users u 
            JOIN posts p ON u.id = p.user_id 
            WHERE u.active = %(active)s
        """,
        "aggregation": "SELECT COUNT(*) as count, active FROM users GROUP BY active",
        "insert": "INSERT INTO users (name, email) VALUES (%(name)s, %(email)s)",
        "update": "UPDATE users SET active = %(active)s WHERE id = %(id)s",
        "delete": "DELETE FROM users WHERE id = %(id)s",
    }


@pytest.fixture
def sample_query_params():
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
