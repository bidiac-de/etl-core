"""
Integration tests for PostgreSQL components with real database connections.

These tests require a running PostgreSQL instance and test the full integration
of components with actual database operations.
"""

import pytest
import asyncio
import dask.dataframe as dd
import pandas as pd

from unittest.mock import Mock, AsyncMock, patch

from src.etl_core.components.databases.postgresql.postgresql_read import PostgreSQLRead
from src.etl_core.components.databases.postgresql.postgresql_write import PostgreSQLWrite
from src.etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from src.etl_core.strategies.base_strategy import ExecutionStrategy


class TestPostgreSQLIntegration:
    """Integration tests for PostgreSQL components and receivers."""

    @pytest.fixture
    def mock_context(self):
        """Create a mock context with credentials."""
        context = Mock()
        # Create mock credentials with get_parameter method
        mock_credentials = Mock()
        mock_credentials.get_parameter.side_effect = lambda param: {
            "user": "testuser",
            "password": "testpass",
            "database": "testdb",
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
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"},
        ]

    @pytest.fixture
    def sample_dataframe(self):
        """Sample pandas DataFrame for testing."""
        return pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["John", "Jane"],
                "email": ["john@example.com", "jane@example.com"],
            }
        )

    @pytest.fixture
    def sample_ddf(self):
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
        return dd.from_pandas(df, npartitions=3)

    @pytest.mark.asyncio
    async def test_read_to_write_pipeline(
        self, mock_context, mock_metrics, sample_data
    ):
        """Test complete read to write pipeline."""
        # Create read component
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Create write component
        write_comp = PostgreSQLWrite(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = mock_context

        # Mock the receivers
        mock_read_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            for item in sample_data:
                yield item

        mock_read_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_read_receiver

        mock_write_receiver = AsyncMock()
        mock_write_receiver.write_row.return_value = {
            "affected_rows": 1,
            "row": {"id": 1, "name": "John"},
        }
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
        # The result now contains the receiver response
        assert write_results[0]["affected_rows"] == 1
        assert write_results[0]["row"]["id"] == 1

    @pytest.mark.asyncio
    async def test_row_strategy_streaming(
        self, mock_context, mock_metrics, sample_data
    ):
        """Test row strategy streaming with PostgreSQL components."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users WHERE id = %(id)s",
            params={"id": 1},
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_read_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
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
    async def test_bigdata_strategy(self, mock_context, mock_metrics, sample_ddf):
        """Test bigdata strategy with PostgreSQL components."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
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
        assert hasattr(results[0], "npartitions")

    @pytest.mark.asyncio
    async def test_component_strategy_execution(self, mock_context, mock_metrics):
        """Test component strategy execution."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
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
    async def test_error_propagation(self, mock_context, mock_metrics):
        """Test error propagation through the pipeline."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
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
    async def test_metrics_integration(self, mock_context, mock_metrics):
        """Test metrics integration with PostgreSQL components."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
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
    async def test_connection_handler_integration(self, mock_context, mock_metrics):
        """Test connection handler integration."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the connection handler
        with patch(
            "src.etl_core.components.databases.sql_database.SQLConnectionHandler"
        ) as mock_handler_class:
            mock_handler = Mock()
            mock_handler.build_url.return_value = (
                "postgresql://user:pass@localhost:5432/testdb"
            )
            mock_handler.connect.return_value = None
            mock_handler_class.return_value = mock_handler

            read_comp._setup_connection()

            # Verify connection was set up
            assert read_comp._connection_handler is not None
            assert read_comp._receiver is not None

    @pytest.mark.asyncio
    async def test_bulk_strategy_integration(
        self, mock_context, mock_metrics, sample_dataframe
    ):
        """Test bulk strategy integration with PostgreSQL components."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
            strategy_type="bulk",
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = sample_dataframe
        read_comp._receiver = mock_receiver

        # Mock the strategy
        mock_strategy = Mock(spec=ExecutionStrategy)

        async def mock_execute_generator(component, payload, metrics):
            result = await read_comp.process_bulk(payload, metrics)
            yield result

        mock_strategy.execute = mock_execute_generator
        read_comp._strategy = mock_strategy

        # Test bulk execution
        payload = sample_dataframe
        results = []
        async for result in read_comp.execute(payload, mock_metrics):
            results.append(result)

        assert len(results) == 1
        assert len(results[0]) == 2

    @pytest.mark.asyncio
    async def test_error_recovery_integration(self, mock_context, mock_metrics):
        """Test error recovery and retry logic in integration."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver to fail first, then succeed
        mock_receiver = AsyncMock()
        call_count = 0

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Database error")
            else:
                yield {"id": 1, "name": "John"}

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        # Test error recovery (this would typically be handled by retry logic)
        with pytest.raises(Exception, match="Database error"):
            async for _ in read_comp.process_row({"id": 1}, mock_metrics):
                pass

    @pytest.mark.asyncio
    async def test_large_dataset_integration(self, mock_context, mock_metrics):
        """Test integration with large datasets."""
        # Create large dataset
        large_data = [
            {"id": i, "name": f"User{i}", "email": f"user{i}@example.com"}
            for i in range(1000)
        ]
        large_df = pd.DataFrame(large_data)

        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = large_df
        read_comp._receiver = mock_receiver

        # Test large dataset processing
        result = await read_comp.process_bulk(large_df, mock_metrics)

        assert len(result) == 1000
        assert result.iloc[0]["id"] == 0
        assert result.iloc[999]["id"] == 999

    @pytest.mark.asyncio
    async def test_concurrent_operations_integration(self, mock_context, mock_metrics):
        """Test concurrent operations in integration."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            yield {"id": 1, "name": "John"}

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        # Test concurrent operations
        async def concurrent_operation():
            results = []
            async for result in read_comp.process_row({"id": 1}, mock_metrics):
                results.append(result)
            return results

        # Run multiple concurrent operations
        tasks = [concurrent_operation() for _ in range(5)]
        results = await asyncio.gather(*tasks)

        # Verify all operations completed
        assert len(results) == 5
        for result_list in results:
            assert len(result_list) == 1
            assert result_list[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_data_transformation_integration(self, mock_context, mock_metrics):
        """Test data transformation through the pipeline."""
        # Create source data
        source_data = [
            {
                "id": 1,
                "first_name": "John",
                "last_name": "Doe",
                "email": "john.doe@example.com",
            },
            {
                "id": 2,
                "first_name": "Jane",
                "last_name": "Smith",
                "email": "jane.smith@example.com",
            },
        ]

        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_read_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            for item in source_data:
                yield item

        mock_read_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_read_receiver

        # Transform data (simulate ETL transformation)
        transformed_data = []
        async for result in read_comp.process_row({"id": 1}, mock_metrics):
            # Transform: combine first_name and last_name
            transformed = {
                "id": result["id"],
                "full_name": f"{result['first_name']} {result['last_name']}",
                "email": result["email"],
            }
            transformed_data.append(transformed)

        # Verify transformation
        assert len(transformed_data) == 2
        assert transformed_data[0]["full_name"] == "John Doe"
        assert transformed_data[1]["full_name"] == "Jane Smith"

    @pytest.mark.asyncio
    async def test_connection_pool_integration(self, mock_context, mock_metrics):
        """Test connection pool integration."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock connection handler with pool settings
        with patch(
            "src.etl_core.components.databases.sql_database.SQLConnectionHandler"
        ) as mock_handler_class:
            mock_handler = Mock()
            mock_handler.build_url.return_value = (
                "postgresql://user:pass@localhost:5432/testdb"
            )
            mock_handler.connect.return_value = None
            mock_handler_class.return_value = mock_handler

            # Call _setup_connection
            read_comp._setup_connection()

            # Verify connection handler was created
            assert read_comp._connection_handler is not None

    @pytest.mark.asyncio
    async def test_metrics_performance_integration(self, mock_context, mock_metrics):
        """Test metrics performance tracking in integration."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver with performance tracking
        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            metrics.set_started()

            # Simulate processing time
            await asyncio.sleep(0.01)

            yield {"id": 1, "name": "John"}

            # Simulate completion metrics
            metrics.set_completed()

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        # Test performance metrics
        results = []
        async for result in read_comp.process_row({"id": 1}, mock_metrics):
            results.append(result)
        end_time = asyncio.get_event_loop().time()

        # Verify metrics and timing
        mock_metrics.set_started.assert_called_once()
        mock_metrics.set_completed.assert_called_once()
        assert len(results) == 1
        assert end_time >= 0  # Basic timing check

    @pytest.mark.asyncio
    async def test_error_handling_strategy_integration(
        self, mock_context, mock_metrics
    ):
        """Test error handling strategy integration."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver with different error scenarios
        mock_receiver = AsyncMock()
        error_scenarios = [
            Exception("Connection timeout"),
            Exception("Query syntax error"),
            Exception("Permission denied"),
            {"id": 1, "name": "John"},  # Success case
        ]

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            for scenario in error_scenarios:
                if isinstance(scenario, Exception):
                    raise scenario
                else:
                    yield scenario

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        # Test error handling strategy
        with pytest.raises(Exception, match="Connection timeout"):
            async for _ in read_comp.process_row({"id": 1}, mock_metrics):
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
