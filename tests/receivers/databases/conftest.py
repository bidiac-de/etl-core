from __future__ import annotations

from unittest.mock import Mock

import dask.dataframe as dd
import pandas as pd
import pytest
from sqlalchemy.engine import Connection as SQLConnection

from etl_core.components.databases.sql_connection_handler import SQLConnectionHandler
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics


@pytest.fixture
def mock_connection_handler() -> Mock:
    """Create a mock connection handler."""
    handler = Mock(spec=SQLConnectionHandler)
    mock_connection = Mock()
    mock_connection.execute.return_value = Mock()
    mock_connection.commit = Mock(return_value=None)
    mock_connection.rollback = Mock(return_value=None)
    mock_connection.__class__ = SQLConnection

    mock_context_manager = Mock()
    mock_context_manager.__enter__ = Mock(return_value=mock_connection)
    mock_context_manager.__exit__ = Mock(return_value=None)
    handler.lease.return_value = mock_context_manager

    return handler


@pytest.fixture
def mock_metrics() -> Mock:
    """Create mock component metrics."""
    metrics = Mock(spec=ComponentMetrics)
    metrics.set_started = Mock()
    metrics.set_completed = Mock()
    metrics.set_failed = Mock()
    return metrics


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Sample pandas DataFrame for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["John", "Jane"],
            "email": ["john@example.com", "jane@example.com"],
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
                "john@example.com",
                "jane@example.com",
                "bob@example.com",
                "alice@example.com",
            ],
        }
    )
    return dd.from_pandas(df, npartitions=2)


@pytest.fixture
def sample_dataframe_with_age() -> pd.DataFrame:
    """Sample pandas DataFrame for testing (with age)."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["John", "Jane", "Bob"],
            "email": ["john@example.com", "jane@example.com", "bob@example.com"],
            "age": [25, 30, 35],
        }
    )


@pytest.fixture
def sample_dask_dataframe_with_age() -> dd.DataFrame:
    """Sample Dask DataFrame for testing (with age)."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["John", "Jane", "Bob", "Alice", "Charlie"],
            "email": [
                "john@example.com",
                "jane@example.com",
                "bob@example.com",
                "alice@example.com",
                "charlie@example.com",
            ],
            "age": [25, 30, 35, 28, 32],
        }
    )
    return dd.from_pandas(df, npartitions=2)
