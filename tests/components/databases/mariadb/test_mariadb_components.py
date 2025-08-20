"""
Tests for MariaDB ETL components.

These tests mock the database connections and test the component logic
without requiring actual MariaDB instances.
"""

import pytest
import pandas as pd

try:
    import dask.dataframe as dd

    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False

from unittest.mock import Mock, AsyncMock, patch

from src.etl_core.components.databases.mariadb.mariadb_read import MariaDBRead
from src.etl_core.components.databases.mariadb.mariadb_write import MariaDBWrite
from src.etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from src.etl_core.strategies.base_strategy import ExecutionStrategy
from src.etl_core.components.databases.mariadb.mariadb import MariaDBComponent
from src.etl_core.components.databases.database import DatabaseComponent

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
        import pandas as pd

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
        if DASK_AVAILABLE:
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
        else:
            # Return a mock if dask is not available
            mock_ddf = Mock()
            mock_ddf.npartitions = 3
            mock_ddf.map_partitions.return_value = mock_ddf
            mock_ddf.compute.return_value = mock_ddf
            return mock_ddf

    def test_mariadb_read_initialization(self):
        """Test MariaDBRead component initialization."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            
            entity_name="users",
            query="SELECT * FROM users",
            params={"limit": 10},
            credentials_id=1,
        )

        assert read_comp.query == "SELECT * FROM users"
        assert read_comp.params == {"limit": 10}
        assert read_comp.credentials_id == 1

    def test_mariadb_write_initialization(self):
        """Test MariaDBWrite component initialization."""
        write_comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="database",
            entity_name="users",
            credentials_id=1,
        )

        assert write_comp.entity_name == "users"
        assert write_comp.credentials_id == 1

    @pytest.mark.asyncio
    async def test_mariadb_read_process_row(
        self, mock_context, mock_metrics, sample_data
    ):
        """Test MariaDBRead process_row method."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            
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

        # Test process_row
        results = []
        async for result in read_comp.process_row({"id": 1}, mock_metrics):
            results.append(result)

        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[1]["id"] == 2

    @pytest.mark.asyncio
    async def test_mariadb_read_process_bulk(
        self, mock_context, mock_metrics, sample_dataframe
    ):
        """Test MariaDBRead process_bulk method."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
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
    async def test_mariadb_read_process_bigdata(
        self, mock_context, mock_metrics, sample_dask_dataframe
    ):
        """Test MariaDBRead process_bigdata method."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bigdata.return_value = sample_dask_dataframe
        read_comp._receiver = mock_receiver

        # Test process_bigdata - returns a Dask DataFrame directly
        result = await read_comp.process_bigdata(sample_dask_dataframe, mock_metrics)

        assert hasattr(result, "npartitions")

    @pytest.mark.asyncio
    async def test_mariadb_write_process_row(self, mock_context, mock_metrics):
        """Test MariaDBWrite process_row method."""
        write_comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="database",
            
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
            results.append(result)

        assert len(results) == 1
        # The result now contains the receiver response
        assert results[0]["affected_rows"] == 1
        assert results[0]["row"]["name"] == "John"

    @pytest.mark.asyncio
    async def test_mariadb_write_process_bulk(
        self, mock_context, mock_metrics, sample_dataframe
    ):
        """Test MariaDBWrite process_bulk method."""
        write_comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="database",
            
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_bulk.return_value = sample_dataframe  # Return the DataFrame
        write_comp._receiver = mock_receiver

        # Test process_bulk - this returns a DataFrame directly, not an async iterator
        result = await write_comp.process_bulk(sample_dataframe, mock_metrics)

        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_mariadb_write_process_bigdata(
        self, mock_context, mock_metrics, sample_dask_dataframe
    ):
        """Test MariaDBWrite process_bigdata method."""
        write_comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="database",
            
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = mock_context

        # Mock the receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_bigdata.return_value = (
            sample_dask_dataframe  # Return the Dask DataFrame
        )
        write_comp._receiver = mock_receiver

        # Test process_bigdata - returns a Dask DataFrame directly
        result = await write_comp.process_bigdata(sample_dask_dataframe, mock_metrics)

        assert hasattr(result, "npartitions")

    def test_mariadb_component_connection_setup(self, mock_context):
        """Test MariaDB component connection setup."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = mock_context

        # Mock the connection handler creation
        with patch(
            "src.etl_core.components.databases.sql_database.SQLConnectionHandler"
        ) as mock_handler_class:
            mock_handler = Mock()
            mock_handler.build_url.return_value = (
                "mysql://user:pass@localhost:3306/testdb"
            )
            mock_handler.connect.return_value = None
            mock_handler_class.return_value = mock_handler

            # Mock the receiver creation
            with patch.object(read_comp, "_create_receiver") as mock_create_receiver:
                mock_receiver = Mock()
                mock_create_receiver.return_value = mock_receiver

                # Call _setup_connection
                read_comp._setup_connection()

                # Verify connection was set up
                assert read_comp._connection_handler is not None
                assert read_comp._receiver is not None

    @pytest.mark.asyncio
    async def test_mariadb_component_error_handling(self, mock_context, mock_metrics):
        """Test MariaDB component error handling."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            
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
    async def test_mariadb_component_strategy_integration(
        self, mock_context, mock_metrics
    ):
        """Test MariaDB component strategy integration."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            
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
            results.append(result)

        assert len(results) == 1
        assert results[0]["id"] == 1

    # NEW TESTS FOR IMPROVED COVERAGE

    def test_mariadb_write_batch_size_configuration(self, mock_context):
        """Test MariaDBWrite batch size configuration."""
        write_comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="database",
            
            entity_name="users",
            credentials_id=1,
            batch_size=500,
        )
        write_comp.context = mock_context

        assert write_comp.batch_size == 500

    @pytest.mark.asyncio
    async def test_mariadb_read_with_complex_params(self, mock_context, mock_metrics):
        """Test MariaDBRead with complex query parameters."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            
            entity_name="users",
            query="SELECT * FROM users WHERE age > %(min_age)s AND city IN %(cities)s",
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
            results.append(result)

        assert len(results) == 1
        assert results[0]["city"] == "Berlin"

    @pytest.mark.asyncio
    async def test_mariadb_write_with_empty_data(self, mock_context, mock_metrics):
        """Test MariaDBWrite with empty data."""
        write_comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="database",
            
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

        result = await write_comp.process_bulk(empty_df, mock_metrics)

        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_mariadb_component_connection_failure(self, mock_context):
        """Test MariaDB component connection failure handling."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            
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
    async def test_mariadb_component_invalid_credentials(self, mock_context):
        """Test MariaDB component with invalid credentials."""
        # Create context with invalid credentials
        invalid_context = Mock()
        invalid_context.get_credentials.side_effect = KeyError("Credentials not found")

        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=999,  # Non-existent credentials
        )
        read_comp.context = invalid_context

        # Test that invalid credentials are handled
        with pytest.raises(KeyError, match="Credentials not found"):
            read_comp._get_credentials()

    @pytest.mark.asyncio
    async def test_mariadb_component_metrics_integration(
        self, mock_context, mock_metrics
    ):
        """Test MariaDB component metrics integration."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            
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
            results.append(result)

        # Verify metrics were called
        mock_metrics.set_started.assert_called_once()
        mock_metrics.set_completed.assert_called_once()
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_mariadb_component_strategy_type_configuration(self, mock_context):
        """Test MariaDB component strategy type configuration."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
            strategy_type="row",  # Explicit strategy type
        )
        read_comp.context = mock_context

        assert read_comp.strategy_type == "row"

    @pytest.mark.asyncio
    async def test_mariadb_component_large_query_handling(
        self, mock_context, mock_metrics
    ):
        """Test MariaDB component with large queries."""
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

        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            
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
            results.append(result)

        assert len(results) == 1
        assert "LEFT JOIN" in read_comp.query

    @pytest.mark.asyncio
    async def test_mariadb_component_special_characters_in_table_name(
        self, mock_context, mock_metrics
    ):
        """Test MariaDB component with special characters in table names."""
        write_comp = MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="database",
            
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
            results.append(result)

        assert len(results) == 1
        assert write_comp.entity_name == "user_profiles_2024"

# Create a concrete MariaDB component instance for edge cases
# where it does not matter if it is read or write
class TestMariaDBComponent(MariaDBComponent):
    def process_row(self, payload, metrics):
        pass

    def process_bulk(self, payload, metrics):
        pass

    def process_bigdata(self, payload, metrics):
        pass

    def test_mariadb_component_charset_collation_defaults(self):
        """Test MariaDB component default charset and collation settings."""

        comp = TestMariaDBComponent(
            name="test_charset",
            description="Test charset component",
            comp_type="database",
            entity_name="users",
            credentials_id=1,
        )

        # Test default values
        assert comp.charset == "utf8mb4"
        assert comp.collation == "utf8mb4_unicode_ci"

        # Test custom values
        comp_custom = TestMariaDBComponent(
            name="test_custom_charset",
            description="Test custom charset component",
            comp_type="database",
            entity_name="users",
            credentials_id=1,
            charset="latin1",
            collation="latin1_swedish_ci",
        )

        assert comp_custom.charset == "latin1"
        assert comp_custom.collation == "latin1_swedish_ci"

    def test_mariadb_component_receiver_creation(self):
        """Test MariaDB component receiver creation."""

        comp = TestMariaDBComponent(
            name="test_receiver",
            description="Test receiver component",
            comp_type="database",
            entity_name="users",
            credentials_id=1,
        )

        # Mock connection handler
        comp._connection_handler = Mock()

        # Test receiver creation
        receiver = comp._create_receiver()
        assert receiver is not None
        assert hasattr(receiver, "read_row")
        assert hasattr(receiver, "write_row")

    def test_mariadb_component_connection_setup_with_session_variables(self):
        """Test MariaDB component connection setup with session variables."""

        comp = TestMariaDBComponent(
            name="test_session_vars",
            description="Test session variables component",
            comp_type="database",
            entity_name="users",
            credentials_id=1,
            charset="utf8",
            collation="utf8_general_ci",
        )

        # Mock connection handler with successful session variable setting
        mock_handler = Mock()
        mock_conn = Mock()
        mock_conn.execute = Mock()
        mock_conn.commit = Mock()

        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_conn)
        mock_context.__exit__ = Mock(return_value=None)
        mock_handler.lease.return_value = mock_context

        comp._connection_handler = mock_handler

        # Call _setup_connection
        comp._setup_connection()

        # Verify session variables were set
        mock_conn.execute.assert_any_call("SET NAMES utf8")
        mock_conn.execute.assert_any_call("SET collation_connection = utf8_general_ci")
        mock_conn.commit.assert_called_once()

    def test_mariadb_component_connection_setup_with_session_variables_failure(self):
        """Test MariaDB component setup when session variable setting fails."""

        comp = TestMariaDBComponent(
            name="test_session_vars_failure",
            description="Test session variables failure component",
            comp_type="database",
            entity_name="users",
            credentials_id=1,
        )

        # Mock connection handler that raises an exception
        mock_handler = Mock()
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Connection failed")

        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_conn)
        mock_context.__exit__ = Mock(return_value=None)
        mock_handler.lease.return_value = mock_context

        comp._connection_handler = mock_handler

        # Mock print to capture warning output
        with patch("builtins.print") as mock_print:
            # Call _setup_connection - should not raise exception
            comp._setup_connection()

            # Verify warning was printed
            mock_print.assert_called_once()
            warning_message = mock_print.call_args[0][0]
            assert (
                "Warning: Could not set MariaDB session variables:" in warning_message
            )
            assert "Connection failed" in warning_message

    def test_mariadb_component_connection_setup_without_connection_handler(self):
        """Test MariaDB component connection setup without connection handler."""

        comp = TestMariaDBComponent(
            name="test_no_handler",
            description="Test no handler component",
            comp_type="database",
            entity_name="users",
            credentials_id=1,
        )

        # No connection handler set
        comp._connection_handler = None

        # Call _setup_connection - should not raise exception
        comp._setup_connection()

        # Should complete without error (no session variables set)

    def test_mariadb_component_various_configurations(self):
        """Test MariaDB component with various configuration combinations."""

        # Test with minimal configuration
        comp_minimal = TestMariaDBComponent(
            name="test_minimal",
            description="Test minimal component",
            comp_type="database",
            entity_name="users",
            credentials_id=1,
        )

        assert comp_minimal.charset == "utf8mb4"
        assert comp_minimal.collation == "utf8mb4_unicode_ci"
        assert comp_minimal.name == "test_minimal"        # Test with custom configuration
        comp_custom = TestMariaDBComponent(
            name="test_custom",
            description="Test custom component",
            comp_type="database",
            entity_name="custom_table",
            credentials_id=999,
            charset="latin1",
            collation="latin1_bin",
        )

        assert comp_custom.charset == "latin1"
        assert comp_custom.collation == "latin1_bin"
        assert comp_custom.entity_name == "custom_table"
        assert comp_custom.credentials_id == 999

    def test_mariadb_component_inheritance_structure(self):
        """Test that MariaDB component has correct inheritance structure."""

        # Verify inheritance
        assert issubclass(MariaDBComponent, DatabaseComponent)

        # Verify abstract methods are implemented
        class TestMariaDBComponent(MariaDBComponent):
            def process_row(self, payload, metrics):
                pass

            def process_bulk(self, payload, metrics):
                pass

            def process_bigdata(self, payload, metrics):
                pass

        comp = TestMariaDBComponent(
            name="test_inheritance",
            description="Test inheritance component",
            comp_type="database",
            entity_name="users",
            credentials_id=1,
        )

        # Verify it has all required methods
        assert hasattr(comp, "process_row")
        assert hasattr(comp, "process_bulk")
        assert hasattr(comp, "process_bigdata")

        # Verify it has MariaDB-specific fields
        assert hasattr(comp, "charset")
        assert hasattr(comp, "collation")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
