"""
Tests for SQL Server ETL components.

These tests mock the database connections and test the component logic
without requiring actual SQL Server instances.
"""

import pytest
import pandas as pd
import dask.dataframe as dd

from unittest.mock import Mock, AsyncMock

from etl_core.components.databases.sqlserver.sqlserver_read import SQLServerRead
from etl_core.components.databases.sqlserver.sqlserver_write import SQLServerWrite
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.strategies.base_strategy import ExecutionStrategy
from etl_core.components.databases.if_exists_strategy import DatabaseOperation
from etl_core.components.databases.sql_connection_handler import SQLConnectionHandler
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType


class TestSQLServerComponents:
    """Test cases for SQL Server components."""

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
        return dd.from_pandas(df, npartitions=2)

    def _create_sqlserver_write_with_schema(self, **kwargs):
        """Helper to create SQLServerWrite component with proper schema."""
        # Set up mock schema for testing
        mock_schema = Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )

        # Merge the schema into kwargs
        if "in_port_schemas" not in kwargs:
            kwargs["in_port_schemas"] = {"in": mock_schema}

        write_comp = SQLServerWrite(**kwargs)

        return write_comp

    def _create_mock_schema(self):
        """Create a mock schema for testing."""
        return Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )

    def test_sqlserver_component_inheritance(self):
        """Test that SQLServerRead inherits correctly."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="read_sqlserver"
        )

        # Check inheritance
        assert isinstance(component, SQLServerRead)

        # Check that it has the expected attributes
        assert hasattr(component, 'charset')
        assert hasattr(component, 'collation')
        assert hasattr(component, 'entity_name')
        assert hasattr(component, 'query')

    def test_sqlserver_component_defaults(self):
        """Test SQL Server component default values."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="read_sqlserver"
        )

        # Check default values
        assert component.charset == "utf8"
        assert component.collation == "SQL_Latin1_General_CP1_CI_AS"
        assert component.entity_name == "test_table"
        assert component.query == ""

    def test_sqlserver_component_custom_values(self):
        """Test SQL Server component with custom values."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="custom_table",
            name="custom_component",
            description="Custom SQL Server component",
            comp_type="read_sqlserver",
            charset="latin1",
            collation="SQL_Latin1_General_CP1_CS_AS",
            query="SELECT * FROM custom_table"
        )

        # Check custom values
        assert component.charset == "latin1"
        assert component.collation == "SQL_Latin1_General_CP1_CS_AS"
        assert component.entity_name == "custom_table"
        assert component.query == "SELECT * FROM custom_table"


class TestSQLServerRead:
    """Test cases for SQL Server read component."""

    @pytest.fixture
    def mock_connection_handler(self):
        """Create a mock connection handler."""
        handler = Mock(spec=SQLConnectionHandler)
        mock_connection = Mock()
        mock_connection.execute.return_value = Mock()
        mock_context_manager = Mock()
        mock_context_manager.__enter__ = Mock(return_value=mock_connection)
        mock_context_manager.__exit__ = Mock(return_value=None)
        handler.lease.return_value = mock_context_manager
        return handler

    @pytest.fixture
    def mock_metrics(self):
        """Create mock component metrics."""
        metrics = Mock(spec=ComponentMetrics)
        metrics.set_started = Mock()
        metrics.set_completed = Mock()
        metrics.set_failed = Mock()
        return metrics

    def test_sqlserver_read_inheritance(self):
        """Test that SQLServerRead inherits correctly."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_read_component",
            description="Test SQL Server read component",
            comp_type="read_sqlserver"
        )

        # Check inheritance
        assert isinstance(component, SQLServerRead)

        # Check that it has the expected attributes
        assert hasattr(component, 'params')
        assert hasattr(component, 'ALLOW_NO_INPUTS')
        assert component.ALLOW_NO_INPUTS is True

    def test_sqlserver_read_defaults(self):
        """Test SQL Server read component default values."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_read_component",
            description="Test SQL Server read component",
            comp_type="read_sqlserver"
        )

        # Check default values
        assert component.params == {}
        assert component.ALLOW_NO_INPUTS is True
        assert component.entity_name == "test_table"
        assert component.query == ""

    def test_sqlserver_read_custom_params(self):
        """Test SQL Server read component with custom parameters."""
        custom_params = {"user_id": 123, "status": "active"}
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_read_component",
            description="Test SQL Server read component",
            comp_type="read_sqlserver",
            query="SELECT * FROM test_table WHERE user_id = :user_id",
            params=custom_params
        )
        
        # Check custom values
        assert component.params == custom_params
        assert component.query == "SELECT * FROM test_table WHERE user_id = :user_id"

    @pytest.mark.parametrize("process_type,expected_results,expected_payload_type", [
        ("row", 2, dict),           # process_row: 2 results, dict payload
        ("bulk", 1, pd.DataFrame),  # process_bulk: 1 result, DataFrame payload
        ("bigdata", 1, dd.DataFrame), # process_bigdata: 1 result, Dask DataFrame payload
    ])
    @pytest.mark.asyncio
    async def test_sqlserver_read_process_methods(
        self, mock_connection_handler, mock_metrics, process_type, expected_results, expected_payload_type
    ):
        """Test SQL Server read component process methods."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_read_component",
            description="Test SQL Server read component",
            comp_type="read_sqlserver"
        )
        
        # Mock the receiver with async methods
        mock_receiver = Mock()
        
        if process_type == "row":
            # Create an async generator for read_row with new signature
            async def mock_read_row_generator(*, entity_name, metrics, connection_handler, batch_size=1000, query=None, params=None):
                yield {"id": 1, "name": "John"}
                yield {"id": 2, "name": "Jane"}

            mock_receiver.read_row = mock_read_row_generator
            component._receiver = mock_receiver
            
            # Test process_row
            results = []
            async for result in component.process_row(None, mock_metrics):
                results.append(result)
            
            # Verify results
            assert len(results) == expected_results
            assert results[0].payload["id"] == 1
            assert results[1].payload["id"] == 2
            
        elif process_type == "bulk":
            mock_dataframe = pd.DataFrame({"id": [1, 2], "name": ["John", "Jane"]})
            async def mock_read_bulk(*, entity_name, metrics, connection_handler, query=None, params=None):
                return mock_dataframe
            mock_receiver.read_bulk = mock_read_bulk
            component._receiver = mock_receiver
            
            # Test process_bulk
            results = []
            async for result in component.process_bulk(None, mock_metrics):
                results.append(result)
            
            # Verify results
            assert len(results) == expected_results
            assert results[0].payload.equals(mock_dataframe)
            
        elif process_type == "bigdata":
            mock_dask_dataframe = dd.from_pandas(
                pd.DataFrame({"id": [1, 2], "name": ["John", "Jane"]}),
                npartitions=1
            )
            async def mock_read_bigdata(*, entity_name, metrics, connection_handler, query=None, params=None):
                return mock_dask_dataframe
            mock_receiver.read_bigdata = mock_read_bigdata
            component._receiver = mock_receiver
            
            # Test process_bigdata
            results = []
            async for result in component.process_bigdata(None, mock_metrics):
                results.append(result)
            
            # Verify results
            assert len(results) == expected_results
            assert results[0].payload.npartitions == 1


class TestSQLServerWrite(TestSQLServerComponents):
    """Test cases for SQL Server write component."""

    @pytest.fixture
    def mock_connection_handler(self):
        """Create a mock connection handler."""
        handler = Mock(spec=SQLConnectionHandler)
        mock_connection = Mock()
        mock_connection.execute.return_value = Mock()
        mock_context_manager = Mock()
        mock_context_manager.__enter__ = Mock(return_value=mock_connection)
        mock_context_manager.__exit__ = Mock(return_value=None)
        handler.lease.return_value = mock_context_manager
        return handler

    @pytest.fixture
    def mock_metrics(self):
        """Create mock component metrics."""
        metrics = Mock(spec=ComponentMetrics)
        metrics.set_started = Mock()
        metrics.set_completed = Mock()
        metrics.set_failed = Mock()
        return metrics

    def test_sqlserver_write_inheritance(self):
        """Test that SQLServerWrite inherits correctly."""
        component = self._create_sqlserver_write_with_schema(
            credentials_id=1,
            entity_name="test_table",
            name="test_write_component",
            description="Test SQL Server write component",
            comp_type="write_sqlserver"
        )

        # Check inheritance
        assert isinstance(component, SQLServerWrite)

        # Check that it has the expected attributes from DatabaseOperationMixin
        assert hasattr(component, 'operation')
        assert hasattr(component, 'where_conditions')

    def test_sqlserver_write_defaults(self):
        """Test SQL Server write component default values."""
        component = self._create_sqlserver_write_with_schema(
            credentials_id=1,
            entity_name="test_table",
            name="test_write_component",
            description="Test SQL Server write component",
            comp_type="write_sqlserver"
        )

        # Check default values from DatabaseOperationMixin
        assert component.operation == DatabaseOperation.INSERT
        assert component.where_conditions == []
        assert component.entity_name == "test_table"

    def test_sqlserver_write_custom_values(self):
        """Test SQL Server write component with custom values."""
        component = self._create_sqlserver_write_with_schema(
            credentials_id=1,
            entity_name="test_table",
            name="test_write_component",
            description="Test SQL Server write component",
            comp_type="write_sqlserver",
            operation=DatabaseOperation.UPSERT,
            where_conditions=["id = :id"]
        )

        # Check custom values
        assert component.operation == DatabaseOperation.UPSERT
        assert component.where_conditions == ["id = :id"]

    @pytest.mark.parametrize("process_type,test_data,expected_payload_type", [
        ("row", {"name": "John", "email": "john@example.com"}, dict),
        ("bulk", pd.DataFrame({"name": ["John", "Jane"], "email": ["john@example.com", "jane@example.com"]}), pd.DataFrame),
        ("bigdata", dd.from_pandas(pd.DataFrame({"name": ["John", "Jane"], "email": ["john@example.com", "jane@example.com"]}), npartitions=1), dd.DataFrame),
    ])
    @pytest.mark.asyncio
    async def test_sqlserver_write_process_methods(
        self, mock_connection_handler, mock_metrics, process_type, test_data, expected_payload_type
    ):
        """Test SQL Server write component process methods."""
        component = self._create_sqlserver_write_with_schema(
            credentials_id=1,
            entity_name="test_table",
            name="test_write_component",
            description="Test SQL Server write component",
            comp_type="write_sqlserver"
        )
        
        # Mock the receiver with async methods
        mock_receiver = Mock()
        
        if process_type == "row":
            async def mock_write_row(*, entity_name, row, metrics, connection_handler, query, table=None):
                return {"affected_rows": 1, "row": row}
            mock_receiver.write_row = mock_write_row
            component._receiver = mock_receiver
            
            # Test process_row
            results = []
            async for result in component.process_row(test_data, mock_metrics):
                results.append(result)
            
            # Verify results
            assert len(results) == 1
            assert results[0].payload["affected_rows"] == 1
            assert results[0].payload["row"]["name"] == "John"
            
        elif process_type == "bulk":
            async def mock_write_bulk(*, entity_name, frame, metrics, connection_handler, query, table=None):
                return frame
            mock_receiver.write_bulk = mock_write_bulk
            component._receiver = mock_receiver
            
            # Test process_bulk
            results = []
            async for result in component.process_bulk(test_data, mock_metrics):
                results.append(result)
            
            # Verify results
            assert len(results) == 1
            assert results[0].payload.equals(test_data)
            
        elif process_type == "bigdata":
            async def mock_write_bigdata(*, entity_name, frame, metrics, connection_handler, query, table=None):
                return frame
            mock_receiver.write_bigdata = mock_write_bigdata
            component._receiver = mock_receiver
            
            # Test process_bigdata
            results = []
            async for result in component.process_bigdata(test_data, mock_metrics):
                results.append(result)
            
            # Verify results
            assert len(results) == 1
            assert results[0].payload.npartitions == 1

    def test_sqlserver_write_port_configuration(self):
        """Test SQL Server write component port configuration."""
        component = self._create_sqlserver_write_with_schema(
            credentials_id=1,
            entity_name="test_table",
            name="test_write_component",
            description="Test SQL Server write component",
            comp_type="write_sqlserver"
        )

        # Check input ports
        assert len(component.INPUT_PORTS) == 1
        assert component.INPUT_PORTS[0].name == "in"
        assert component.INPUT_PORTS[0].required is True
        assert component.INPUT_PORTS[0].fanin == "many"

        # Check output ports
        assert len(component.OUTPUT_PORTS) == 1
        assert component.OUTPUT_PORTS[0].name == "out"
        assert component.OUTPUT_PORTS[0].required is False
        assert component.OUTPUT_PORTS[0].fanout == "many"



    @pytest.mark.asyncio
    async def test_sqlserver_component_error_handling(
        self, mock_context, mock_metrics, sample_data
    ):
        """Test SQL Server component error handling."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="read_sqlserver"
        )
        component.context = mock_context
        
        # Initialize the component to build objects
        component._build_objects()

        # Mock receiver to raise error
        mock_receiver = AsyncMock()
        
        # Create an async generator that raises an exception
        async def error_generator():
            raise Exception("Database error")
        
        mock_receiver.read_row.return_value = error_generator()
        component._receiver = mock_receiver

        # Test error propagation
        with pytest.raises(Exception):
            async for _ in component.process_row(None, mock_metrics):
                pass

    @pytest.mark.asyncio
    async def test_sqlserver_component_strategy_integration(
        self, mock_context, mock_metrics, sample_dataframe
    ):
        """Test SQL Server component strategy integration."""
        # Test read component with different strategies
        read_comp = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver"
        )
        read_comp.context = mock_context

        # Mock receiver for read operations
        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = sample_dataframe
        mock_receiver.read_row = AsyncMock()
        mock_receiver.read_bigdata.return_value = dd.from_pandas(sample_dataframe, npartitions=1)
        read_comp._receiver = mock_receiver

        # Test bulk strategy
        results = []
        async for result in read_comp.process_bulk(None, mock_metrics):
            results.append(result)
        assert len(results) == 1
        assert results[0].payload.equals(sample_dataframe)

        # Test bigdata strategy
        results = []
        async for result in read_comp.process_bigdata(None, mock_metrics):
            results.append(result)
        assert len(results) == 1
        assert results[0].payload.npartitions == 1

    @pytest.mark.asyncio
    async def test_sqlserver_write_with_empty_data(self, mock_context, mock_metrics):
        """Test SQL Server write component with empty data."""
        component = self._create_sqlserver_write_with_schema(
            credentials_id=1,
            entity_name="test_table",
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver"
        )
        component.context = mock_context

        # Mock receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = {"affected_rows": 0, "row": {}}
        component._receiver = mock_receiver

        # Test with empty/None data
        results = []
        async for result in component.process_row({}, mock_metrics):
            results.append(result)
        assert len(results) == 1

    def test_sqlserver_component_metrics_integration(self, mock_context, mock_metrics):
        """Test SQL Server component metrics integration."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="read_sqlserver"
        )
        component.context = mock_context

        # Mock receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = pd.DataFrame({"id": [1, 2]})
        component._receiver = mock_receiver

        # Test that metrics are passed to receiver
        assert component._receiver is not None

    @pytest.mark.asyncio
    async def test_sqlserver_component_connection_handler_integration(
        self, mock_context, mock_metrics
    ):
        """Test SQL Server component connection handler integration."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="read_sqlserver"
        )
        component.context = mock_context

        # Mock receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = pd.DataFrame({"id": [1, 2]})
        component._receiver = mock_receiver

        # Test that connection handler is properly accessed
        assert hasattr(component, 'connection_handler')

    @pytest.mark.asyncio
    async def test_sqlserver_read_with_complex_params(
        self, mock_context, mock_metrics
    ):
        """Test SQL Server read component with complex parameters."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            query="SELECT * FROM test_table WHERE status = :status AND created_at > :created_at",
            params={"status": "active", "created_at": "2023-01-01"}
        )
        component.context = mock_context

        # Mock receiver
        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = pd.DataFrame({"id": [1, 2], "status": ["active", "active"]})
        component._receiver = mock_receiver

        # Test with complex parameters
        results = []
        async for result in component.process_bulk(None, mock_metrics):
            results.append(result)
        assert len(results) == 1

    def test_sqlserver_component_charset_collation_defaults(self):
        """Test SQL Server component charset and collation defaults."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="read_sqlserver"
        )

        # Check default values
        assert component.charset == "utf8"
        assert component.collation == "SQL_Latin1_General_CP1_CI_AS"

    @pytest.mark.asyncio
    async def test_sqlserver_component_special_characters_in_data(
        self, mock_context, mock_metrics
    ):
        """Test SQL Server component with special characters in data."""
        component = self._create_sqlserver_write_with_schema(
            credentials_id=1,
            entity_name="test_table",
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver"
        )
        component.context = mock_context

        # Test data with special characters
        special_data = {
            "name": "José María",
            "email": "jose.maria@café.com",
            "description": "Special chars: äöüßñéèêë"
        }

        # Mock receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = {"affected_rows": 1, "row": special_data}
        component._receiver = mock_receiver

        # Test write with special characters
        results = []
        async for result in component.process_row(special_data, mock_metrics):
            results.append(result)
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_sqlserver_component_numeric_data_types(self, mock_context, mock_metrics):
        """Test SQL Server component with various numeric data types."""
        component = self._create_sqlserver_write_with_schema(
            credentials_id=1,
            entity_name="numeric_table",
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver"
        )
        component.context = mock_context

        # Test data with different numeric types
        numeric_data = {
            "integer": 42,
            "float": 3.14159,
            "decimal": 123.456,
            "negative": -100,
        }

        # Mock receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = {"affected_rows": 1, "row": numeric_data}
        component._receiver = mock_receiver

        # Test write with numeric data
        results = []
        async for result in component.process_row(numeric_data, mock_metrics):
            results.append(result)
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_sqlserver_component_boolean_data_types(self, mock_context, mock_metrics):
        """Test SQL Server component with boolean data types."""
        component = self._create_sqlserver_write_with_schema(
            credentials_id=1,
            entity_name="boolean_table",
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver"
        )
        component.context = mock_context

        # Test data with boolean values
        boolean_data = {"is_active": True, "is_deleted": False, "has_permission": True}

        # Mock receiver
        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = {"affected_rows": 1, "row": boolean_data}
        component._receiver = mock_receiver

        # Test write with boolean data
        results = []
        async for result in component.process_row(boolean_data, mock_metrics):
            results.append(result)
        assert len(results) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

