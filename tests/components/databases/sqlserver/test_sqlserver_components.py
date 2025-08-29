"""
Tests for SQL Server components.

These tests verify that SQL Server components work correctly
and inherit all the expected functionality from the base classes.
"""

import pytest
import pandas as pd
import dask.dataframe as dd
from unittest.mock import Mock, patch, AsyncMock

from etl_core.components.databases.sqlserver.sqlserver_read import SQLServerRead
from etl_core.components.databases.sqlserver.sqlserver_write import SQLServerWrite
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.components.databases.sql_connection_handler import SQLConnectionHandler


class TestSQLServerComponent:
    """Test cases for SQL Server base component."""

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
        assert hasattr(component, 'if_exists')
        assert hasattr(component, 'bulk_chunk_size')
        assert hasattr(component, 'bigdata_partition_chunk_size')

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
        assert component.if_exists == "append"
        assert component.bulk_chunk_size == 50_000
        assert component.bigdata_partition_chunk_size == 50_000

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
            query="SELECT * FROM custom_table",
            if_exists="replace",
            bulk_chunk_size=25_000,
            bigdata_partition_chunk_size=100_000
        )
        
        # Check custom values
        assert component.charset == "latin1"
        assert component.collation == "SQL_Latin1_General_CP1_CS_AS"
        assert component.entity_name == "custom_table"
        assert component.query == "SELECT * FROM custom_table"
        assert component.if_exists == "replace"
        assert component.bulk_chunk_size == 25_000
        assert component.bigdata_partition_chunk_size == 100_000


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

    @pytest.mark.asyncio
    async def test_sqlserver_read_process_row(self, mock_connection_handler, mock_metrics):
        """Test SQL Server read component process_row method."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_read_component",
            description="Test SQL Server read component",
            comp_type="read_sqlserver"
        )
        
        # Mock the receiver with async methods
        mock_receiver = Mock()
        mock_receiver.read_row = AsyncMock(return_value=iter([
            {"id": 1, "name": "John"},
            {"id": 2, "name": "Jane"}
        ]))
        component._receiver = mock_receiver
        
        # Test process_row
        results = []
        async for result in component.process_row(None, mock_metrics):
            results.append(result)
        
        # Verify results
        assert len(results) == 2
        assert results[0].payload["id"] == 1
        assert results[1].payload["id"] == 2

    @pytest.mark.asyncio
    async def test_sqlserver_read_process_bulk(self, mock_connection_handler, mock_metrics):
        """Test SQL Server read component process_bulk method."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_read_component",
            description="Test SQL Server read component",
            comp_type="read_sqlserver"
        )
        
        # Mock the receiver with async methods
        mock_receiver = Mock()
        mock_dataframe = pd.DataFrame({"id": [1, 2], "name": ["John", "Jane"]})
        mock_receiver.read_bulk = AsyncMock(return_value=mock_dataframe)
        component._receiver = mock_receiver
        
        # Test process_bulk
        results = []
        async for result in component.process_bulk(None, mock_metrics):
            results.append(result)
        
        # Verify results
        assert len(results) == 1
        assert results[0].payload.equals(mock_dataframe)

    @pytest.mark.asyncio
    async def test_sqlserver_read_process_bigdata(self, mock_connection_handler, mock_metrics):
        """Test SQL Server read component process_bigdata method."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_read_component",
            description="Test SQL Server read component",
            comp_type="read_sqlserver"
        )
        
        # Mock the receiver with async methods
        mock_receiver = Mock()
        mock_dask_dataframe = dd.from_pandas(
            pd.DataFrame({"id": [1, 2], "name": ["John", "Jane"]}),
            npartitions=1
        )
        mock_receiver.read_bigdata = AsyncMock(return_value=mock_dask_dataframe)
        component._receiver = mock_receiver
        
        # Test process_bigdata
        results = []
        async for result in component.process_bigdata(None, mock_metrics):
            results.append(result)
        
        # Verify results
        assert len(results) == 1
        assert results[0].payload.npartitions == 1


class TestSQLServerWrite:
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
        component = SQLServerWrite(
            credentials_id=1,
            entity_name="test_table",
            name="test_write_component",
            description="Test SQL Server write component",
            comp_type="write_sqlserver"
        )
        
        # Check inheritance
        assert isinstance(component, SQLServerWrite)
        
        # Check that it has the expected attributes
        assert hasattr(component, 'if_exists')
        assert hasattr(component, 'bulk_chunk_size')
        assert hasattr(component, 'bigdata_partition_chunk_size')

    def test_sqlserver_write_defaults(self):
        """Test SQL Server write component default values."""
        component = SQLServerWrite(
            credentials_id=1,
            entity_name="test_table",
            name="test_write_component",
            description="Test SQL Server write component",
            comp_type="write_sqlserver"
        )
        
        # Check default values
        assert component.if_exists == "append"
        assert component.bulk_chunk_size == 50_000
        assert component.bigdata_partition_chunk_size == 50_000
        assert component.entity_name == "test_table"
        assert component.query == ""

    def test_sqlserver_write_custom_values(self):
        """Test SQL Server write component with custom values."""
        component = SQLServerWrite(
            credentials_id=1,
            entity_name="test_table",
            name="test_write_component",
            description="Test SQL Server write component",
            comp_type="write_sqlserver",
            if_exists="replace",
            bulk_chunk_size=25_000,
            bigdata_partition_chunk_size=100_000
        )
        
        # Check custom values
        assert component.if_exists == "replace"
        assert component.bulk_chunk_size == 25_000
        assert component.bigdata_partition_chunk_size == 100_000

    @pytest.mark.asyncio
    async def test_sqlserver_write_process_row(self, mock_connection_handler, mock_metrics):
        """Test SQL Server write component process_row method."""
        component = SQLServerWrite(
            credentials_id=1,
            entity_name="test_table",
            name="test_write_component",
            description="Test SQL Server write component",
            comp_type="write_sqlserver"
        )
        
        # Mock the receiver with async methods
        mock_receiver = Mock()
        mock_receiver.write_row = AsyncMock(return_value={"affected_rows": 1, "row": {"name": "John"}})
        component._receiver = mock_receiver
        
        # Test process_row
        test_row = {"name": "John", "email": "john@example.com"}
        results = []
        async for result in component.process_row(test_row, mock_metrics):
            results.append(result)
        
        # Verify results
        assert len(results) == 1
        assert results[0].payload["affected_rows"] == 1
        assert results[0].payload["row"]["name"] == "John"

    @pytest.mark.asyncio
    async def test_sqlserver_write_process_bulk(self, mock_connection_handler, mock_metrics):
        """Test SQL Server write component process_bulk method."""
        component = SQLServerWrite(
            credentials_id=1,
            entity_name="test_table",
            name="test_write_component",
            description="Test SQL Server write component",
            comp_type="write_sqlserver"
        )
        
        # Mock the receiver with async methods
        mock_receiver = Mock()
        test_dataframe = pd.DataFrame({"name": ["John", "Jane"], "email": ["john@example.com", "jane@example.com"]})
        mock_receiver.write_bulk = AsyncMock(return_value=test_dataframe)
        component._receiver = mock_receiver
        
        # Test process_bulk
        results = []
        async for result in component.process_bulk(test_dataframe, mock_metrics):
            results.append(result)
        
        # Verify results
        assert len(results) == 1
        assert results[0].payload.equals(test_dataframe)

    @pytest.mark.asyncio
    async def test_sqlserver_write_process_bigdata(self, mock_connection_handler, mock_metrics):
        """Test SQL Server write component process_bigdata method."""
        component = SQLServerWrite(
            credentials_id=1,
            entity_name="test_table",
            name="test_write_component",
            description="Test SQL Server write component",
            comp_type="write_sqlserver"
        )
        
        # Mock the receiver with async methods
        mock_receiver = Mock()
        test_dask_dataframe = dd.from_pandas(
            pd.DataFrame({"name": ["John", "Jane"], "email": ["john@example.com", "jane@example.com"]}),
            npartitions=1
        )
        mock_receiver.write_bigdata = AsyncMock(return_value=test_dask_dataframe)
        component._receiver = mock_receiver
        
        # Test process_bigdata
        results = []
        async for result in component.process_bigdata(test_dask_dataframe, mock_metrics):
            results.append(result)
        
        # Verify results
        assert len(results) == 1
        assert results[0].payload.npartitions == 1

    def test_sqlserver_write_port_configuration(self):
        """Test SQL Server write component port configuration."""
        component = SQLServerWrite(
            credentials_id=1,
            entity_name="test_table",
            name="test_write_component",
            description="Test SQL Server write component",
            comp_type="write_sqlserver"
        )
        
        # Check port configuration
        assert len(component.INPUT_PORTS) == 1
        assert component.INPUT_PORTS[0].name == "in"
        assert component.INPUT_PORTS[0].required is True
        assert component.INPUT_PORTS[0].fanin == "many"
        
        assert len(component.OUTPUT_PORTS) == 1
        assert component.OUTPUT_PORTS[0].name == "out"
        assert component.OUTPUT_PORTS[0].required is False
        assert component.OUTPUT_PORTS[0].fanout == "many"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

