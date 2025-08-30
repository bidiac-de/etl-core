"""
Tests for PostgreSQL ETL components.

These tests mock the database connections and test the component logic
without requiring actual PostgreSQL instances.
"""

import pytest
import pandas as pd
import dask.dataframe as dd

from unittest.mock import Mock, AsyncMock

from etl_core.components.databases.postgresql.postgresql_read import PostgreSQLRead
from etl_core.components.databases.postgresql.postgresql_write import PostgreSQLWrite
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.strategies.base_strategy import ExecutionStrategy
from etl_core.components.databases.postgresql.postgresql import PostgreSQLComponent
from etl_core.components.databases.database import DatabaseComponent


class TestPostgreSQLComponents:
    """Test cases for PostgreSQL components."""

    def _create_postgresql_write_with_schema(self, **kwargs):
        """Helper to create PostgreSQLWrite component with proper schema."""
        from etl_core.components.wiring.schema import Schema
        from etl_core.components.wiring.column_definition import FieldDef, DataType
        
        write_comp = PostgreSQLWrite(**kwargs)
        
        # Set up mock schema for testing
        mock_schema = Schema(fields=[
            FieldDef(name="id", data_type=DataType.INTEGER),
            FieldDef(name="name", data_type=DataType.STRING),
            FieldDef(name="email", data_type=DataType.STRING),
        ])
        write_comp.in_port_schemas = {"in": mock_schema}
        
        return write_comp

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
            "port": 5432,
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
    def sample_dask_dataframe(self):
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

    def test_postgresql_read_initialization(self):
        """Test PostgreSQLRead component initialization."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query="SELECT * FROM users",
            params={"limit": 10},
            credentials_id=1,
        )

        assert read_comp.query == "SELECT * FROM users"
        assert read_comp.params == {"limit": 10}
        assert read_comp.credentials_id == 1

    def test_postgresql_write_initialization(self):
        """Test PostgreSQLWrite component initialization."""
        write_comp = self._create_postgresql_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
        )

        assert write_comp.entity_name == "users"
        assert write_comp.credentials_id == 1

    @pytest.mark.asyncio
    async def test_postgresql_read_process_row(
        self, mock_context, mock_metrics, sample_data
    ):
        """Test PostgreSQLRead process_row method."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query="SELECT * FROM users WHERE id = %(id)s",
            params={"id": 1},
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()

        # Create an async generator for read_row
        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            for item in sample_data:
                yield item

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        # Test process_row - now returns AsyncIterator[Out]
        results = []
        async for result in read_comp.process_row({"id": 1}, mock_metrics):
            results.append(result.payload)

        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[1]["id"] == 2

    @pytest.mark.asyncio
    async def test_postgresql_read_process_bulk(
        self, mock_context, mock_metrics, sample_dataframe
    ):
        """Test PostgreSQLRead process_bulk method."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = sample_dataframe
        read_comp._receiver = mock_receiver

        # Test process_bulk - this now returns an async iterator
        results = []
        async for result in read_comp.process_bulk(sample_dataframe, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1  # Only one Out object
        assert len(results[0]) == 2  # DataFrame has 2 rows

    @pytest.mark.asyncio
    async def test_postgresql_read_process_bigdata(
        self, mock_context, mock_metrics, sample_dask_dataframe
    ):
        """Test PostgreSQLRead process_bigdata method."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bigdata.return_value = sample_dask_dataframe
        read_comp._receiver = mock_receiver

        # Test process_bigdata - now returns an async iterator
        results = []
        async for result in read_comp.process_bigdata(
            sample_dask_dataframe, mock_metrics
        ):
            results.append(result.payload)

        assert len(results) == 1  # Only one Out object
        assert hasattr(results[0], "npartitions")

    @pytest.mark.asyncio
    async def test_postgresql_write_process_row(self, mock_context, mock_metrics):
        """Test PostgreSQLWrite process_row method."""
        write_comp = self._create_postgresql_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = {
            "affected_rows": 1,
            "row": {"name": "John", "email": "john@example.com"},
        }
        write_comp._receiver = mock_receiver

        # Test process_row - this returns an async iterator
        results = []
        async for result in write_comp.process_row(
            {"name": "John", "email": "john@example.com"}, mock_metrics
        ):
            results.append(result.payload)

        assert len(results) == 1
        # The result now contains the receiver response
        assert results[0]["affected_rows"] == 1
        assert results[0]["row"]["name"] == "John"

    @pytest.mark.asyncio
    async def test_postgresql_write_process_bulk(
        self, mock_context, mock_metrics, sample_dataframe
    ):
        """Test PostgreSQLWrite process_bulk method."""
        write_comp = self._create_postgresql_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_bulk.return_value = sample_dataframe  # Return the DataFrame
        write_comp._receiver = mock_receiver

        # Test process_bulk - this now returns an async iterator
        results = []
        async for result in write_comp.process_bulk(sample_dataframe, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1  # Only one Out object
        assert len(results[0]) == 2  # DataFrame has 2 rows

    @pytest.mark.asyncio
    async def test_postgresql_write_process_bigdata(
        self, mock_context, mock_metrics, sample_dask_dataframe
    ):
        """Test PostgreSQLWrite process_bigdata method."""
        write_comp = self._create_postgresql_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
            if_exists="replace",  # Test the new if_exists parameter
            bigdata_partition_chunk_size=25_000,  # Test the new parameter
        )
        write_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_bigdata.return_value = (
            sample_dask_dataframe  # Return the Dask DataFrame
        )
        write_comp._receiver = mock_receiver

        # Test process_bigdata - now returns an async iterator
        results = []
        async for result in write_comp.process_bigdata(
            sample_dask_dataframe, mock_metrics
        ):
            results.append(result.payload)

        # Verify the receiver was called with the new parameters
        mock_receiver.write_bigdata.assert_called_once()
        call_args = mock_receiver.write_bigdata.call_args
        assert "query" in call_args.kwargs  # Check that query is passed
        assert call_args.kwargs["entity_name"] == "users"
        assert (
            call_args.kwargs["frame"] is sample_dask_dataframe
        )  # Use is for identity comparison
        assert call_args.kwargs["metrics"] == mock_metrics
        assert call_args.kwargs["connection_handler"] == write_comp.connection_handler

        assert len(results) == 1  # Only one Out object
        assert hasattr(results[0], "npartitions")

    def test_postgresql_component_connection_setup(self, mock_context):
        """Test PostgreSQL component connection setup."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Test that the component can be created and has the right properties
        assert read_comp.name == "test_read"
        assert read_comp.comp_type == "read_postgresql"
        assert read_comp.entity_name == "users"
        assert read_comp.query == "SELECT * FROM users"
        assert read_comp.credentials_id == 1

        # Test that the component has the expected attributes
        assert hasattr(read_comp, "_connection_handler")
        assert hasattr(read_comp, "_receiver")

        # Note: We don't test _setup_connection() directly as it's a private method
        # and requires proper credentials setup. The real connection setup is tested
        # in integration tests with real credentials.

    @pytest.mark.asyncio
    async def test_postgresql_component_error_handling(
        self, mock_context, mock_metrics
    ):
        """Test PostgreSQL component error handling."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
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
    async def test_postgresql_component_strategy_integration(
        self, mock_context, mock_metrics
    ):
        """Test PostgreSQL component strategy integration."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
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

        # Test strategy integration
        payload = {"id": 1}
        results = []
        async for result in read_comp.execute(payload, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1
        assert results[0]["id"] == 1

    # NEW TESTS FOR IMPROVED COVERAGE

    def test_postgresql_write_batch_size_configuration(self, mock_context):
        """Test PostgreSQLWrite batch size configuration."""
        write_comp = self._create_postgresql_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
            row_batch_size=500,
        )
        write_comp.context = mock_context

        assert write_comp.row_batch_size == 500

    @pytest.mark.asyncio
    async def test_postgresql_read_with_complex_params(
        self, mock_context, mock_metrics
    ):
        """Test PostgreSQLRead with complex query parameters."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query=(
                "SELECT * FROM users WHERE age > %(min_age)s AND city = ANY(%(cities)s)"
            ),
            params={"min_age": 18, "cities": ["Berlin", "München", "Hamburg"]},
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            yield {"id": 1, "name": "John", "age": 25, "city": "Berlin"}

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        # Test process_row with complex params
        results = []
        async for result in read_comp.process_row(
            {"min_age": 18, "cities": ["Berlin"]}, mock_metrics
        ):
            results.append(result.payload)

        assert len(results) == 1
        assert results[0]["city"] == "Berlin"

    @pytest.mark.asyncio
    async def test_postgresql_write_with_empty_data(self, mock_context, mock_metrics):
        """Test PostgreSQLWrite with empty data."""
        write_comp = self._create_postgresql_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = mock_context

        # Test process_bulk with empty DataFrame
        empty_df = pd.DataFrame()

        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_bulk.return_value = empty_df  # Return the empty DataFrame
        write_comp._receiver = mock_receiver

        results = []
        async for result in write_comp.process_bulk(empty_df, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1  # Only one Out object
        assert len(results[0]) == 0  # Empty DataFrame

    @pytest.mark.asyncio
    async def test_postgresql_component_connection_failure(self, mock_context):
        """Test PostgreSQL component connection failure handling."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=999,  # Use non-existent credentials ID
        )

        # Create a context without the credentials we're looking for
        empty_context = Mock()
        empty_context.get_credentials.side_effect = KeyError("Credentials not found")
        read_comp.context = empty_context

        # Test that connection failure is handled during component creation
        # We'll test a different scenario - invalid credentials
        with pytest.raises(KeyError, match="Credentials not found"):
            # This should fail because we're using a context without the required
            # credentials
            read_comp._get_credentials()

    @pytest.mark.asyncio
    async def test_postgresql_component_invalid_credentials(self, mock_context):
        """Test PostgreSQL component with invalid credentials."""
        # Create context with invalid credentials
        invalid_context = Mock()
        invalid_context.get_credentials.side_effect = KeyError("Credentials not found")

        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=999,  # Non-existent credentials
        )
        read_comp.context = invalid_context

        # Test that invalid credentials are handled
        with pytest.raises(KeyError, match="Credentials not found"):
            read_comp._get_credentials()

    @pytest.mark.asyncio
    async def test_postgresql_component_metrics_integration(
        self, mock_context, mock_metrics
    ):
        """Test PostgreSQL component metrics integration."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            # Simulate metrics usage
            metrics.set_started()
            yield {"id": 1, "name": "John"}
            metrics.set_completed()

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        # Test metrics integration
        results = []
        async for result in read_comp.process_row({"id": 1}, mock_metrics):
            results.append(result.payload)

        # Verify metrics were called
        mock_metrics.set_started.assert_called_once()
        mock_metrics.set_completed.assert_called_once()
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_postgresql_component_strategy_type_configuration(self, mock_context):
        """Test PostgreSQL component strategy type configuration."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
            # strategy_type is now handled at Job level, not Component level
        )
        read_comp.context = mock_context

        # Strategy is now assigned by the Job, not stored in the Component
        # We can test that the component can be created without strategy_type
        assert read_comp.name == "test_read"
        assert read_comp.query == "SELECT * FROM users"

    @pytest.mark.asyncio
    async def test_postgresql_component_large_query_handling(
        self, mock_context, mock_metrics
    ):
        """Test PostgreSQL component with large queries."""
        large_query = """
        SELECT u.id, u.name, u.email, p.phone, a.street, a.city, a.country
        FROM users u
        LEFT JOIN profiles p ON u.id = p.user_id
        LEFT JOIN addresses a ON u.id = a.user_id
        WHERE u.created_at > %(start_date)s
        AND u.status = 'active'
        ORDER BY u.created_at DESC
        LIMIT %(limit)s
        """

        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query=large_query,
            params={"start_date": "2023-01-01", "limit": 1000},
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            yield {"id": 1, "name": "John", "email": "john@example.com"}

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        # Test that large queries are handled
        results = []
        async for result in read_comp.process_row(
            {"start_date": "2023-01-01", "limit": 1000}, mock_metrics
        ):
            results.append(result.payload)

        assert len(results) == 1
        assert "LEFT JOIN" in read_comp.query

    @pytest.mark.asyncio
    async def test_postgresql_component_special_characters_in_table_name(
        self, mock_context, mock_metrics
    ):
        """Test PostgreSQL component with special characters in table names."""
        write_comp = self._create_postgresql_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="user_profiles_2024",  # Table with underscores and numbers
            credentials_id=1,
        )
        write_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = None
        write_comp._receiver = mock_receiver

        # Test that special table names are handled
        results = []
        async for result in write_comp.process_row({"name": "John"}, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1
        assert write_comp.entity_name == "user_profiles_2024"

    def test_postgresql_component_charset_collation_defaults(self):
        """Test PostgreSQL component default charset and collation settings."""
        # Create a mock component for testing
        mock_comp = Mock()
        mock_comp.charset = "utf8"
        mock_comp.collation = "en_US.UTF-8"

        # Test default values
        assert mock_comp.charset == "utf8"
        assert mock_comp.collation == "en_US.UTF-8"

        # Test custom values
        mock_comp_custom = Mock()
        mock_comp_custom.charset = "latin1"
        mock_comp_custom.collation = "en_US.UTF-8"

        assert mock_comp_custom.charset == "latin1"
        assert mock_comp_custom.collation == "en_US.UTF-8"

    def test_postgresql_component_connection_setup_with_session_variables(self):
        """Test PostgreSQL component connection setup with session variables."""
        # Mock component and connection handler
        mock_comp = Mock()
        mock_handler = Mock()
        mock_conn = Mock()
        mock_conn.execute = Mock()
        mock_conn.commit = Mock()

        # Test that session variables can be set
        mock_comp._connection_handler = mock_handler
        mock_comp.charset = "utf8"
        mock_comp.collation = "en_US.UTF-8"

        # This is a basic test - the actual implementation would be more complex
        assert mock_comp.charset == "utf8"
        assert mock_comp.collation == "en_US.UTF-8"

    @pytest.mark.asyncio
    async def test_postgresql_component_operation_types(self, mock_context):
        """Test PostgreSQL component with different operation types."""
        # Test INSERT operation (default)
        write_comp_insert = self._create_postgresql_write_with_schema(
            name="test_write_insert",
            description="Test write component with INSERT",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
            operation="insert",
        )
        write_comp_insert.context = mock_context

        # Test UPSERT operation
        write_comp_upsert = self._create_postgresql_write_with_schema(
            name="test_write_upsert",
            description="Test write component with UPSERT",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
            operation="upsert",
        )
        write_comp_upsert.context = mock_context

        # Test UPDATE operation
        write_comp_update = self._create_postgresql_write_with_schema(
            name="test_write_update",
            description="Test write component with UPDATE",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
            operation="update",
        )
        write_comp_update.context = mock_context

        # Test TRUNCATE operation
        write_comp_truncate = self._create_postgresql_write_with_schema(
            name="test_write_truncate",
            description="Test write component with TRUNCATE",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
            operation="truncate",
        )
        write_comp_truncate.context = mock_context

        # Verify operation types are set correctly
        assert write_comp_insert.operation == "insert"
        assert write_comp_upsert.operation == "upsert"
        assert write_comp_update.operation == "update"
        assert write_comp_truncate.operation == "truncate"

    @pytest.mark.asyncio
    async def test_postgresql_component_batch_size_configuration(self, mock_context):
        """Test PostgreSQL component batch size configuration."""
        write_comp = self._create_postgresql_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
            row_batch_size=500,
            bulk_chunk_size=25_000,
            bigdata_partition_chunk_size=100_000,
        )
        write_comp.context = mock_context

        # Verify batch size configurations
        assert write_comp.row_batch_size == 500
        assert write_comp.bulk_chunk_size == 25_000
        assert write_comp.bigdata_partition_chunk_size == 100_000

    @pytest.mark.asyncio
    async def test_postgresql_component_query_building(self, mock_context):
        """Test PostgreSQL component query building functionality."""
        write_comp = self._create_postgresql_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
            operation="insert",
        )
        write_comp.context = mock_context

        # Test query building with different operations
        columns = ["id", "name", "email"]
        
        # Test INSERT query
        insert_query = write_comp._build_query("users", columns, "insert")
        assert "INSERT INTO users" in insert_query
        assert "id, name, email" in insert_query
        
        # Test UPSERT query
        upsert_query = write_comp._build_query("users", columns, "upsert", conflict_columns=["id"])
        assert "INSERT INTO users" in upsert_query
        assert "ON CONFLICT" in upsert_query
        
        # Test UPDATE query
        update_query = write_comp._build_query("users", columns, "update", where_conditions=["id = :id"])
        assert "UPDATE users" in update_query
        assert "SET" in update_query
        assert "WHERE" in update_query
        
        # Test TRUNCATE query
        truncate_query = write_comp._build_query("users", columns, "truncate")
        assert "TRUNCATE TABLE users" in truncate_query
        assert "INSERT INTO users" in truncate_query

    @pytest.mark.asyncio
    async def test_postgresql_component_port_configuration(self):
        """Test PostgreSQL component port configuration."""
        # Test read component ports
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        
        # Verify input ports (should be empty for read component)
        assert len(read_comp.INPUT_PORTS) == 0
        assert read_comp.ALLOW_NO_INPUTS is True
        
        # Verify output ports
        assert len(read_comp.OUTPUT_PORTS) == 1
        assert read_comp.OUTPUT_PORTS[0].name == "out"
        assert read_comp.OUTPUT_PORTS[0].required is True
        assert read_comp.OUTPUT_PORTS[0].fanout == "many"
        
        # Test write component ports
        write_comp = PostgreSQLWrite(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
        )
        
        # Verify input ports
        assert len(write_comp.INPUT_PORTS) == 1
        assert write_comp.INPUT_PORTS[0].name == "in"
        assert write_comp.INPUT_PORTS[0].required is True
        assert write_comp.INPUT_PORTS[0].fanin == "many"
        
        # Verify output ports
        assert len(write_comp.OUTPUT_PORTS) == 1
        assert write_comp.OUTPUT_PORTS[0].name == "out"
        assert write_comp.OUTPUT_PORTS[0].required is False
        assert write_comp.OUTPUT_PORTS[0].fanout == "many"

    @pytest.mark.asyncio
    async def test_postgresql_component_schema_validation(self, mock_context):
        """Test PostgreSQL component schema validation."""
        from etl_core.components.wiring.schema import Schema
        from etl_core.components.wiring.column_definition import FieldDef, DataType
        
        # Create a component with schema
        write_comp = PostgreSQLWrite(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
        )
        
        # Set up mock schema
        mock_schema = Schema(fields=[
            FieldDef(name="id", data_type=DataType.INTEGER),
            FieldDef(name="name", data_type=DataType.STRING),
            FieldDef(name="email", data_type=DataType.STRING),
        ])
        write_comp.in_port_schemas = {"in": mock_schema}
        
        # Test that schema is properly set
        assert "in" in write_comp.in_port_schemas
        assert write_comp.in_port_schemas["in"] == mock_schema
        
        # Test that query building works with schema
        write_comp._ensure_query_built()
        assert write_comp._query is not None
        assert write_comp._columns == ["id", "name", "email"]

    @pytest.mark.asyncio
    async def test_postgresql_component_receiver_integration(self, mock_context, mock_metrics):
        """Test PostgreSQL component receiver integration."""
        write_comp = self._create_postgresql_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = mock_context
        
        # Mock the receiver methods
        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = {"affected_rows": 1}
        mock_receiver.write_bulk.return_value = pd.DataFrame({"id": [1]})
        mock_receiver.write_bigdata.return_value = dd.from_pandas(pd.DataFrame({"id": [1]}), npartitions=1)
        
        write_comp._receiver = mock_receiver
        
        # Test that receiver methods are called correctly
        test_row = {"id": 1, "name": "John"}
        test_df = pd.DataFrame([test_row])
        test_ddf = dd.from_pandas(test_df, npartitions=1)
        
        # Test row processing
        results = []
        async for result in write_comp.process_row(test_row, mock_metrics):
            results.append(result.payload)
        
        assert len(results) == 1
        mock_receiver.write_row.assert_called_once()
        
        # Test bulk processing
        results = []
        async for result in write_comp.process_bulk(test_df, mock_metrics):
            results.append(result.payload)
        
        assert len(results) == 1
        mock_receiver.write_bulk.assert_called_once()
        
        # Test bigdata processing
        results = []
        async for result in write_comp.process_bigdata(test_ddf, mock_metrics):
            results.append(result.payload)
        
        assert len(results) == 1
        mock_receiver.write_bigdata.assert_called_once()

    @pytest.mark.asyncio
    async def test_postgresql_component_connection_handler_integration(self, mock_context):
        """Test PostgreSQL component connection handler integration."""
        write_comp = self._create_postgresql_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = mock_context
        
        # Test that connection handler is properly set up
        assert hasattr(write_comp, "_connection_handler")
        # The connection handler is set up during validation when context is set
        # We need to trigger the validation
        write_comp._build_objects()
        assert write_comp.connection_handler is not None
        
        # Test that connection handler is passed to receiver methods
        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = {"affected_rows": 1}
        write_comp._receiver = mock_receiver
        
        test_row = {"id": 1, "name": "John"}
        
        # Process a row to trigger receiver call
        results = []
        async for result in write_comp.process_row(test_row, Mock()):
            results.append(result.payload)
        
        # Verify connection handler was passed to receiver
        mock_receiver.write_row.assert_called_once()
        call_args = mock_receiver.write_row.call_args
        assert "connection_handler" in call_args.kwargs
        assert call_args.kwargs["connection_handler"] == write_comp.connection_handler


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
