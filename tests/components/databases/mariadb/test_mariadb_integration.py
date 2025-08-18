"""
Integration tests for MariaDB ETL components.

These tests test the complete flow from component to receiver
without requiring actual MariaDB instances.
"""

import pytest
import asyncio
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import AsyncIterator, Dict, Any

from src.components.databases.mariadb.mariadb_read import MariaDBRead
from src.components.databases.mariadb.mariadb_write import MariaDBWrite
from src.receivers.databases.mariadb.mariadb_receiver import MariaDBReceiver
from src.components.databases.sql_connection_handler import SQLConnectionHandler
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.strategies.base_strategy import ExecutionStrategy
from src.components.schema import Schema


class TestMariaDBIntegration:
    """Integration tests for MariaDB components and receivers."""

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
    def sample_ddf(self):
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

    @pytest.mark.asyncio
    async def test_read_to_write_pipeline(self, mock_context, mock_metrics, sample_data, mock_schema):
        """Test complete read to write pipeline."""
        # Create read component
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
        
        # Create write component
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
        
        # Mock the receivers
        mock_read_receiver = AsyncMock()
        async def mock_read_row_generator(query, params, metrics):
            for item in sample_data:
                yield item
        mock_read_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_read_receiver
        
        mock_write_receiver = AsyncMock()
        mock_write_receiver.write_row.return_value = None  # write_row doesn't return anything
        write_comp._receiver = mock_write_receiver
        
        # Test read operation
        read_results = []
        async for result in read_comp.process_row({"id": 1}, mock_metrics):
            read_results.append(result)
        
        assert len(read_results) == 2
        
        # Test write operation
        write_results = []
        async for result in write_comp.process_row(read_results[0], mock_metrics):
            write_results.append(result)
        
        assert len(write_results) == 1
        assert write_results[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_row_strategy_streaming(self, mock_context, mock_metrics, sample_data, mock_schema):
        """Test row strategy streaming with MariaDB components."""
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
        mock_read_receiver = AsyncMock()
        async def mock_read_row_generator(query, params, metrics):
            for item in sample_data:
                yield item
        mock_read_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_read_receiver
        
        # Mock the strategy
        mock_strategy = Mock(spec=ExecutionStrategy)
        async def mock_execute_generator(component, payload, metrics):
            async for item in read_comp.process_row(payload, metrics):
                yield item
        mock_strategy.execute = mock_execute_generator
        read_comp._strategy = mock_strategy
        
        # Test streaming execution
        payload = {"id": 1}
        results = []
        async for result in read_comp.execute(payload, mock_metrics):
            results.append(result)
        
        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[1]["id"] == 2

    @pytest.mark.asyncio
    async def test_bigdata_strategy(self, mock_context, mock_metrics, sample_ddf, mock_schema):
        """Test bigdata strategy with MariaDB components."""
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
        mock_receiver.read_bigdata.return_value = sample_ddf
        read_comp._receiver = mock_receiver
        
        # Mock the strategy
        mock_strategy = Mock(spec=ExecutionStrategy)
        async def mock_execute_generator(component, payload, metrics):
            result = await read_comp.process_bigdata(payload, metrics)
            yield result
        mock_strategy.execute = mock_execute_generator
        read_comp._strategy = mock_strategy
        
        # Test bigdata execution
        results = []
        async for result in read_comp.execute(sample_ddf, mock_metrics):
            results.append(result)
        
        assert len(results) == 1
        assert hasattr(results[0], 'npartitions')

    @pytest.mark.asyncio
    async def test_component_strategy_execution(self, mock_context, mock_metrics, mock_schema):
        """Test component strategy execution."""
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
        
        # Test execution
        payload = {"id": 1}
        results = []
        async for result in read_comp.execute(payload, mock_metrics):
            results.append(result)
        
        assert len(results) == 1
        assert results[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_error_propagation(self, mock_context, mock_metrics, mock_schema):
        """Test error propagation through the pipeline."""
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
        
        # Mock the receiver to raise an error
        mock_receiver = AsyncMock()
        mock_receiver.read_row.side_effect = Exception("Database error")
        read_comp._receiver = mock_receiver
        
        # Test that error is propagated
        with pytest.raises(Exception):
            async for _ in read_comp.process_row({"id": 1}, mock_metrics):
                pass

    @pytest.mark.asyncio
    async def test_metrics_integration(self, mock_context, mock_metrics, mock_schema):
        """Test metrics integration with MariaDB components."""
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
            # Call metrics methods to simulate real usage
            metrics.set_started()
            yield {"id": 1, "name": "John"}
            metrics.set_completed()
        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver
        
        # Test metrics integration
        payload = {"id": 1}
        results = []
        async for result in read_comp.process_row(payload, mock_metrics):
            results.append(result)
        
        # Verify metrics were called
        mock_metrics.set_started.assert_called_once()
        mock_metrics.set_completed.assert_called_once()
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_connection_handler_integration(self, mock_context, mock_metrics, mock_schema):
        """Test connection handler integration."""
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

        # Mock the connection handler
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
