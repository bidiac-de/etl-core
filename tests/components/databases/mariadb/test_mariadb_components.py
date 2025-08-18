"""
Tests for MariaDB ETL components.

These tests mock the database connections and test the component logic
without requiring actual MariaDB instances.
"""

import pytest
import asyncio
import pandas as pd
try:
    import dask.dataframe as dd
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import AsyncIterator, Dict, Any

from src.components.databases.mariadb.mariadb_read import MariaDBRead
from src.components.databases.mariadb.mariadb_write import MariaDBWrite
from src.components.databases.mariadb.mariadb import MariaDBComponent
from src.components.databases.sql_connection_handler import SQLConnectionHandler
from src.receivers.databases.mariadb.mariadb_receiver import MariaDBReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.strategies.base_strategy import ExecutionStrategy
from src.components.schema import Schema


class TestMariaDBComponents:
    """Test cases for MariaDB components."""

    @pytest.fixture
    def mock_context(self):
        """Create a mock context with credentials."""
        context = Mock()
        # Create mock credentials with get_parameter method
        mock_credentials = Mock()
        mock_credentials.get_parameter.side_effect = lambda param: {
            "user": "testuser",
            "password": "testpass",
            "database": "testdb"
        }.get(param)
        mock_credentials.decrypted_password = "testpass"
        context.get_credentials.return_value = mock_credentials
        return context

    @pytest.fixture
    def mock_metrics(self):
        """Create mock component metrics."""
        metrics = Mock(spec=ComponentMetrics)
        metrics.set_started = Mock()
        metrics.set_completed = Mock()
        metrics.set_failed = Mock()
        return metrics

    @pytest.fixture
    def mock_schema(self):
        """Create mock schema."""
        schema = Mock(spec=Schema)
        return schema

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"}
        ]

    @pytest.fixture
    def sample_dataframe(self):
        """Sample pandas DataFrame for testing."""
        import pandas as pd
        return pd.DataFrame({
            "id": [1, 2],
            "name": ["John", "Jane"],
            "email": ["john@example.com", "jane@example.com"]
        })

    @pytest.fixture
    def sample_dask_dataframe(self):
        """Sample Dask DataFrame for testing."""
        try:
            import dask.dataframe as dd
            import pandas as pd
            df = pd.DataFrame({
                "id": [1, 2, 3, 4],
                "name": ["John", "Jane", "Bob", "Alice"],
                "email": ["john@example.com", "jane@example.com", "bob@example.com", "alice@example.com"]
            })
            return dd.from_pandas(df, npartitions=3)
        except ImportError:
            # Return a mock if dask is not available
            mock_ddf = Mock()
            mock_ddf.npartitions = 3
            mock_ddf.map_partitions.return_value = mock_ddf
            mock_ddf.compute.return_value = mock_ddf
            return mock_ddf

    def test_mariadb_read_initialization(self, mock_schema):
        """Test MariaDBRead component initialization."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            schema=mock_schema,
            database="testdb",
            table="users",
            query="SELECT * FROM users",
            params={"limit": 10},
            host="localhost",
            port=3306,
            credentials_id=1
        )
        
        assert read_comp.query == "SELECT * FROM users"
        assert read_comp.params == {"limit": 10}
        assert read_comp.host == "localhost"
        assert read_comp.port == 3306
        assert read_comp.credentials_id == 1

    def test_mariadb_write_initialization(self, mock_schema):
        """Test MariaDBWrite component initialization."""
        write_comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="database",
            schema=mock_schema,
            database="testdb",
            table="users",
            host="localhost",
            port=3306,
            credentials_id=1
        )
        
        assert write_comp.table == "users"
        assert write_comp.host == "localhost"
        assert write_comp.port == 3306
        assert write_comp.credentials_id == 1

    @pytest.mark.asyncio
    async def test_mariadb_read_process_row(self, mock_context, mock_metrics, sample_data, mock_schema):
        """Test MariaDBRead process_row method."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            schema=mock_schema,
            database="testdb",
            table="users",
            query="SELECT * FROM users WHERE id = %(id)s",
            params={"id": 1},
            host="localhost",
            port=3306,
            credentials_id=1
        )
        read_comp.context = mock_context
        
        # Mock the receiver
        mock_receiver = AsyncMock()
        # Create an async generator for read_row
        async def mock_read_row_generator(query, params, metrics):
            for item in sample_data:
                yield item
        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver
        
        # Test process_row
        results = []
        async for result in read_comp.process_row({"id": 1}, mock_metrics):
            results.append(result)
        
        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[1]["id"] == 2

    @pytest.mark.asyncio
    async def test_mariadb_read_process_bulk(self, mock_context, mock_metrics, sample_dataframe, mock_schema):
        """Test MariaDBRead process_bulk method."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            schema=mock_schema,
            database="testdb",
            table="users",
            query="SELECT * FROM users",
            host="localhost",
            port=3306,
            credentials_id=1
        )
        read_comp.context = mock_context
        
        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = sample_dataframe
        read_comp._receiver = mock_receiver
        
        # Test process_bulk - this returns a DataFrame directly, not an async iterator
        result = await read_comp.process_bulk(sample_dataframe, mock_metrics)
        
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_mariadb_read_process_bigdata(self, mock_context, mock_metrics, sample_dask_dataframe, mock_schema):
        """Test MariaDBRead process_bigdata method."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            schema=mock_schema,
            database="testdb",
            table="users",
            query="SELECT * FROM users",
            host="localhost",
            port=3306,
            credentials_id=1
        )
        read_comp.context = mock_context
        
        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bigdata.return_value = sample_dask_dataframe
        read_comp._receiver = mock_receiver
        
        # Test process_bigdata - this returns a Dask DataFrame directly, not an async iterator
        result = await read_comp.process_bigdata(sample_dask_dataframe, mock_metrics)
        
        assert hasattr(result, 'npartitions')

    @pytest.mark.asyncio
    async def test_mariadb_write_process_row(self, mock_context, mock_metrics, mock_schema):
        """Test MariaDBWrite process_row method."""
        write_comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="database",
            schema=mock_schema,
            database="testdb",
            table="users",
            host="localhost",
            port=3306,
            credentials_id=1
        )
        write_comp.context = mock_context
        
        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = None  # write_row doesn't return anything
        write_comp._receiver = mock_receiver
        
        # Test process_row - this returns an async iterator
        results = []
        async for result in write_comp.process_row({"name": "John", "email": "john@example.com"}, mock_metrics):
            results.append(result)
        
        assert len(results) == 1
        assert results[0]["name"] == "John"

    @pytest.mark.asyncio
    async def test_mariadb_write_process_bulk(self, mock_context, mock_metrics, sample_dataframe, mock_schema):
        """Test MariaDBWrite process_bulk method."""
        write_comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="database",
            schema=mock_schema,
            database="testdb",
            table="users",
            host="localhost",
            port=3306,
            credentials_id=1
        )
        write_comp.context = mock_context
        
        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_bulk.return_value = None  # write_bulk doesn't return anything
        write_comp._receiver = mock_receiver
        
        # Test process_bulk - this returns a DataFrame directly, not an async iterator
        result = await write_comp.process_bulk(sample_dataframe, mock_metrics)
        
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_mariadb_write_process_bigdata(self, mock_context, mock_metrics, sample_dask_dataframe, mock_schema):
        """Test MariaDBWrite process_bigdata method."""
        write_comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="database",
            schema=mock_schema,
            database="testdb",
            table="users",
            host="localhost",
            port=3306,
            credentials_id=1
        )
        write_comp.context = mock_context
        
        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_bigdata.return_value = None  # write_bigdata doesn't return anything
        write_comp._receiver = mock_receiver
        
        # Test process_bigdata - this returns a Dask DataFrame directly, not an async iterator
        result = await write_comp.process_bigdata(sample_dask_dataframe, mock_metrics)
        
        assert hasattr(result, 'npartitions')

    def test_mariadb_write_build_insert_query(self, mock_context, mock_schema):
        """Test MariaDBWrite build_insert_query method."""
        write_comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="database",
            schema=mock_schema,
            database="testdb",
            table="users",
            host="localhost",
            port=3306,
            credentials_id=1
        )
        write_comp.context = mock_context

        # Mock the connection handler and receiver to avoid real DB connection
        with patch('src.components.databases.database.SQLConnectionHandler') as mock_handler_class:
            mock_handler = Mock()
            mock_handler.build_url.return_value = "mysql://user:pass@localhost:3306/testdb"
            mock_handler.connect.return_value = None
            mock_handler_class.return_value = mock_handler
            
            # Mock the receiver creation
            with patch.object(write_comp, '_create_receiver') as mock_create_receiver:
                mock_receiver = Mock()
                mock_create_receiver.return_value = mock_receiver
                
                # Call _setup_connection
                write_comp._setup_connection()
                
                # Verify the insert query building works
                query = write_comp._build_insert_query(["name", "email"])
                assert "INSERT INTO users" in query
                assert "name, email" in query
                assert "VALUES" in query

    def test_mariadb_component_connection_setup(self, mock_context, mock_schema):
        """Test MariaDB component connection setup."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            schema=mock_schema,
            database="testdb",
            table="users",
            query="SELECT * FROM users",
            host="localhost",
            port=3306,
            credentials_id=1
        )
        read_comp.context = mock_context

        # Mock the connection handler creation
        with patch('src.components.databases.database.SQLConnectionHandler') as mock_handler_class:
            mock_handler = Mock()
            mock_handler.build_url.return_value = "mysql://user:pass@localhost:3306/testdb"
            mock_handler.connect.return_value = None
            mock_handler_class.return_value = mock_handler
            
            # Mock the receiver creation
            with patch.object(read_comp, '_create_receiver') as mock_create_receiver:
                mock_receiver = Mock()
                mock_create_receiver.return_value = mock_receiver
                
                # Call _setup_connection
                read_comp._setup_connection()
                
                # Verify connection was set up
                assert read_comp._connection_handler is not None
                assert read_comp._receiver is not None

    @pytest.mark.asyncio
    async def test_mariadb_component_error_handling(self, mock_context, mock_schema, mock_metrics):
        """Test MariaDB component error handling."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            schema=mock_schema,
            database="testdb",
            table="users",
            query="SELECT * FROM users",
            host="localhost",
            port=3306,
            credentials_id=1
        )
        read_comp.context = mock_context
        
        # Mock the receiver to raise an error when read_row is called
        mock_receiver = AsyncMock()
        mock_receiver.read_row.side_effect = Exception("Database error")
        read_comp._receiver = mock_receiver
        
        # Test that error is handled gracefully - we need to actually call the method
        with pytest.raises(Exception):
            # This will trigger the error when we try to read
            async for result in read_comp.process_row({"id": 1}, mock_metrics):
                pass

    @pytest.mark.asyncio
    async def test_mariadb_component_strategy_integration(self, mock_context, mock_metrics, mock_schema):
        """Test MariaDB component strategy integration."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            schema=mock_schema,
            database="testdb",
            table="users",
            query="SELECT * FROM users",
            host="localhost",
            port=3306,
            credentials_id=1
        )
        read_comp.context = mock_context
        
        # Mock the receiver
        mock_receiver = AsyncMock()
        async def mock_read_row_generator(query, params, metrics):
            yield {"id": 1, "name": "John"}
        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver
        
        # Mock the strategy
        mock_strategy = Mock(spec=ExecutionStrategy)
        async def mock_execute_generator(component, payload, metrics):
            async for item in read_comp.process_row(payload, metrics):
                yield item
        mock_strategy.execute = mock_execute_generator
        read_comp._strategy = mock_strategy
        
        # Test strategy integration
        payload = {"id": 1}
        results = []
        async for result in read_comp.execute(payload, mock_metrics):
            results.append(result)
        
        assert len(results) == 1
        assert results[0]["id"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
