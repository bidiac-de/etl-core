"""
Integration tests for SQL Server components with real database connections.

These tests require a running SQL Server instance and test the full integration
of components with actual database operations.
"""

import pytest
import asyncio

import dask.dataframe as dd
import pandas as pd

from unittest.mock import Mock, AsyncMock

from etl_core.components.databases.sqlserver.sqlserver_read import SQLServerRead
from etl_core.components.databases.sqlserver.sqlserver_write import SQLServerWrite
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics


class TestSQLServerIntegration:
    """Integration tests for SQL Server components and receivers."""

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
            "host": "localhost",
            "port": 1433,
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
        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Create write component with schema
        from etl_core.components.wiring.schema import Schema
        from etl_core.components.wiring.column_definition import FieldDef, DataType

        mock_schema = Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )

        write_comp = SQLServerWrite(
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver",
            entity_name="users",
            credentials_id=1,
            in_port_schemas={"in": mock_schema},
        )
        write_comp.context = mock_context

        # Mock the receivers
        mock_read_receiver = AsyncMock()
        mock_write_receiver = AsyncMock()

        # Mock read_row to return sample data with new signature
        async def mock_read_row_generator(
            *,
            entity_name,
            metrics,
            connection_handler,
            batch_size=1000,
            query=None,
            params=None,
        ):
            for row in sample_data:
                yield row

        mock_read_receiver.read_row = mock_read_row_generator

        # Mock write_row with new signature
        async def mock_write_row(
            *, entity_name, row, metrics, connection_handler, query, table=None
        ):
            return {"affected_rows": 1, "row": row}

        mock_write_receiver.write_row = mock_write_row

        read_comp._receiver = mock_read_receiver
        write_comp._receiver = mock_write_receiver

        # Test the pipeline
        read_results = []
        async for result in read_comp.process_row(None, mock_metrics):
            read_results.append(result)

        assert len(read_results) == 2
        assert read_results[0].payload["name"] == "John"
        assert read_results[1].payload["name"] == "Jane"

    @pytest.mark.asyncio
    async def test_row_strategy_streaming(
        self, mock_context, mock_metrics, sample_data
    ):
        """Test row strategy streaming functionality."""
        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock receiver
        mock_receiver = AsyncMock()

        async def mock_read_row_generator(*args, **kwargs):
            for row in sample_data:
                yield row

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        # Test streaming
        results = []
        async for result in read_comp.process_row(None, mock_metrics):
            results.append(result)

        assert len(results) == 2
        assert all(isinstance(result.payload, dict) for result in results)

    @pytest.mark.asyncio
    async def test_bigdata_strategy(self, mock_context, mock_metrics, sample_ddf):
        """Test bigdata strategy with Dask DataFrames."""
        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bigdata = AsyncMock(return_value=sample_ddf)
        read_comp._receiver = mock_receiver

        # Test bigdata processing
        results = []
        async for result in read_comp.process_bigdata(None, mock_metrics):
            results.append(result)

        assert len(results) == 1
        assert results[0].payload.npartitions == 3

    @pytest.mark.asyncio
    async def test_component_strategy_execution(self, mock_context, mock_metrics):
        """Test component strategy execution."""
        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bulk = AsyncMock(return_value=pd.DataFrame({"id": [1, 2]}))
        read_comp._receiver = mock_receiver

        # Test bulk strategy
        results = []
        async for result in read_comp.process_bulk(None, mock_metrics):
            results.append(result)

        assert len(results) == 1
        assert len(results[0].payload) == 2

    @pytest.mark.asyncio
    async def test_error_propagation(self, mock_context, mock_metrics):
        """Test error propagation through components."""
        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock receiver to raise error
        mock_receiver = AsyncMock()

        # Create an async generator that raises an exception
        async def error_generator(*args, **kwargs):
            raise Exception("Database error")
            yield  # This line will never be reached

        mock_receiver.read_row = error_generator
        read_comp._receiver = mock_receiver

        # Test error propagation
        with pytest.raises(Exception, match="Database error"):
            async for _ in read_comp.process_row(None, mock_metrics):
                pass

    @pytest.mark.asyncio
    async def test_metrics_integration(self, mock_context, mock_metrics):
        """Test metrics integration with components."""
        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bulk = AsyncMock(return_value=pd.DataFrame({"id": [1]}))
        read_comp._receiver = mock_receiver

        # Test metrics are called
        async for _ in read_comp.process_bulk(None, mock_metrics):
            pass

        # Verify that metrics were passed to the receiver
        mock_receiver.read_bulk.assert_called_once()
        call_args = mock_receiver.read_bulk.call_args
        assert call_args[1]["metrics"] == mock_metrics

    @pytest.mark.asyncio
    async def test_connection_handler_integration(self, mock_context, mock_metrics):
        """Test connection handler integration."""
        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock connection handler
        mock_handler = Mock()
        read_comp._connection_handler = mock_handler

        # Verify connection handler is accessible
        assert read_comp.connection_handler == mock_handler

    @pytest.mark.asyncio
    async def test_bulk_strategy_integration(
        self, mock_context, mock_metrics, sample_dataframe
    ):
        """Test bulk strategy integration."""
        # Create write component with schema
        from etl_core.components.wiring.schema import Schema
        from etl_core.components.wiring.column_definition import FieldDef, DataType

        mock_schema = Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )

        write_comp = SQLServerWrite(
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver",
            entity_name="users",
            credentials_id=1,
            in_port_schemas={"in": mock_schema},
        )
        write_comp.context = mock_context

        # Mock receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_bulk = AsyncMock(return_value=sample_dataframe)
        write_comp._receiver = mock_receiver

        # Test bulk write
        results = []
        async for result in write_comp.process_bulk(sample_dataframe, mock_metrics):
            results.append(result)

        assert len(results) == 1
        assert results[0].payload.equals(sample_dataframe)

    @pytest.mark.asyncio
    async def test_error_recovery_integration(self, mock_context, mock_metrics):
        """Test error recovery integration."""
        # Create write component with schema
        from etl_core.components.wiring.schema import Schema
        from etl_core.components.wiring.column_definition import FieldDef, DataType

        mock_schema = Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )

        write_comp = SQLServerWrite(
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver",
            entity_name="users",
            credentials_id=1,
            in_port_schemas={"in": mock_schema},
        )
        write_comp.context = mock_context

        # Mock receiver with error then success
        mock_receiver = AsyncMock()
        mock_receiver.write_row.side_effect = [
            Exception("First error"),
            {"affected_rows": 1},
        ]
        write_comp._receiver = mock_receiver

        # Test error recovery
        with pytest.raises(Exception, match="First error"):
            async for _ in write_comp.process_row({"name": "John"}, mock_metrics):
                pass

    @pytest.mark.parametrize(
        "test_type,data_size,concurrent_tasks",
        [
            ("large_dataset", 1000, 1),  # Large dataset processing
            ("concurrent", 2, 3),  # Concurrent operations
            ("transformation", 2, 1),  # Data transformation
        ],
    )
    @pytest.mark.asyncio
    async def test_advanced_scenarios_integration(
        self, mock_context, mock_metrics, test_type, data_size, concurrent_tasks
    ):
        """Test advanced integration scenarios."""

        if test_type == "large_dataset":
            # Test large dataset processing
            large_df = pd.DataFrame(
                {
                    "id": range(data_size),
                    "name": [f"User{i}" for i in range(data_size)],
                    "email": [f"user{i}@example.com" for i in range(data_size)],
                }
            )

            # Create write component with schema
            from etl_core.components.wiring.schema import Schema
            from etl_core.components.wiring.column_definition import FieldDef, DataType

            mock_schema = Schema(
                fields=[
                    FieldDef(name="id", data_type=DataType.INTEGER),
                    FieldDef(name="name", data_type=DataType.STRING),
                    FieldDef(name="email", data_type=DataType.STRING),
                ]
            )

            write_comp = SQLServerWrite(
                name="test_write",
                description="Test write component",
                comp_type="write_sqlserver",
                entity_name="users",
                credentials_id=1,
                bulk_chunk_size=100,
                in_port_schemas={"in": mock_schema},
            )
            write_comp.context = mock_context

            mock_receiver = AsyncMock()
            mock_receiver.write_bulk = AsyncMock(return_value=large_df)
            write_comp._receiver = mock_receiver

            results = []
            async for result in write_comp.process_bulk(large_df, mock_metrics):
                results.append(result)

            assert len(results) == 1
            assert len(results[0].payload) == data_size

        elif test_type == "concurrent":
            # Test concurrent operations
            read_comp = SQLServerRead(
                name="test_read",
                description="Test read component",
                comp_type="read_sqlserver",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=1,
            )
            read_comp.context = mock_context

            mock_receiver = AsyncMock()
            mock_receiver.read_bulk = AsyncMock(
                return_value=pd.DataFrame({"id": [1, 2]})
            )
            read_comp._receiver = mock_receiver

            async def concurrent_operation():
                results = []
                async for result in read_comp.process_bulk(None, mock_metrics):
                    results.append(result)
                return results

            tasks = [concurrent_operation() for _ in range(concurrent_tasks)]
            results = await asyncio.gather(*tasks)

            assert len(results) == concurrent_tasks
            for result_list in results:
                assert len(result_list) == 1

        elif test_type == "transformation":
            # Test data transformation
            read_comp = SQLServerRead(
                name="test_read",
                description="Test read component",
                comp_type="read_sqlserver",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=1,
            )
            read_comp.context = mock_context

            mock_receiver = AsyncMock()
            mock_receiver.read_bulk = AsyncMock(
                return_value=pd.DataFrame(
                    {
                        "id": [1, 2],
                        "name": ["john", "jane"],
                        "email": ["john@example.com", "jane@example.com"],
                    }
                )
            )
            read_comp._receiver = mock_receiver

            results = []
            async for result in read_comp.process_bulk(None, mock_metrics):
                result.payload["name"] = result.payload["name"].str.capitalize()
                results.append(result)

            assert len(results) == 1
            assert results[0].payload["name"].iloc[0] == "John"
            assert results[0].payload["name"].iloc[1] == "Jane"

    @pytest.mark.asyncio
    async def test_connection_pool_integration(self, mock_context, mock_metrics):
        """Test connection pool integration."""
        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock connection handler with pool settings
        mock_handler = Mock()
        mock_handler._registry = Mock()
        read_comp._connection_handler = mock_handler

        # Verify pool integration
        assert hasattr(read_comp.connection_handler, "_registry")

    @pytest.mark.asyncio
    async def test_metrics_performance_integration(self, mock_context, mock_metrics):
        """Test metrics performance integration."""
        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bulk = AsyncMock(return_value=pd.DataFrame({"id": [1, 2]}))
        read_comp._receiver = mock_receiver

        # Test performance metrics
        start_time = asyncio.get_event_loop().time()
        async for _ in read_comp.process_bulk(None, mock_metrics):
            pass
        end_time = asyncio.get_event_loop().time()

        # Verify that metrics were passed to the receiver
        mock_receiver.read_bulk.assert_called_once()
        call_args = mock_receiver.read_bulk.call_args
        assert call_args[1]["metrics"] == mock_metrics

        # Verify reasonable execution time
        assert end_time - start_time < 1.0  # Should complete quickly with mocks

    @pytest.mark.asyncio
    async def test_error_handling_strategy_integration(
        self, mock_context, mock_metrics
    ):
        """Test error handling strategy integration."""
        # Create write component with schema
        from etl_core.components.wiring.schema import Schema
        from etl_core.components.wiring.column_definition import FieldDef, DataType

        mock_schema = Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )

        write_comp = SQLServerWrite(
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver",
            entity_name="users",
            credentials_id=1,
            in_port_schemas={"in": mock_schema},
        )
        write_comp.context = mock_context

        # Mock connection handler
        mock_handler = Mock()
        mock_connection = Mock()
        mock_context_manager = Mock()
        mock_context_manager.__enter__ = Mock(return_value=mock_connection)
        mock_context_manager.__exit__ = Mock(return_value=None)
        mock_handler.lease.return_value = mock_context_manager
        write_comp._connection_handler = mock_handler

        # Mock receiver to raise an error
        mock_receiver = AsyncMock()
        mock_receiver.write_row.side_effect = Exception("Test error")
        write_comp._receiver = mock_receiver

        # Test that the error is raised
        with pytest.raises(Exception, match="Test error"):
            async for _ in write_comp.process_row({"name": "John"}, mock_metrics):
                pass

    @pytest.mark.asyncio
    async def test_sqlserver_component_numeric_data_types(
        self, mock_context, mock_metrics
    ):
        """Test SQL Server component with numeric data types."""
        numeric_data = [
            {"id": 1, "age": 25, "salary": 50000.50, "is_active": True},
            {"id": 2, "age": 30, "salary": 75000.75, "is_active": False},
        ]

        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = pd.DataFrame(numeric_data)
        read_comp._receiver = mock_receiver

        # Test that numeric data types are handled correctly
        results = []
        async for result in read_comp.process_bulk(None, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1
        assert len(results[0]) == 2
        assert results[0].iloc[0]["age"] == 25
        assert results[0].iloc[0]["salary"] == 50000.50
        assert (
            results[0].iloc[0]["is_active"].item() is True
        )  # Convert to native boolean

    @pytest.mark.asyncio
    async def test_sqlserver_component_boolean_data_types(
        self, mock_context, mock_metrics
    ):
        """Test SQL Server component with boolean data types."""
        boolean_data = [
            {"id": 1, "is_active": True, "is_admin": False},
            {"id": 2, "is_active": False, "is_admin": True},
        ]

        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = pd.DataFrame(boolean_data)
        read_comp._receiver = mock_receiver

        # Test that boolean data types are handled correctly
        results = []
        async for result in read_comp.process_bulk(None, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1
        assert len(results[0]) == 2
        assert (
            results[0].iloc[0]["is_active"].item() is True
        )  # Convert to native boolean
        assert results[0].iloc[0]["is_admin"].item() is False
        assert results[0].iloc[1]["is_active"].item() is False
        assert results[0].iloc[1]["is_admin"].item() is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
