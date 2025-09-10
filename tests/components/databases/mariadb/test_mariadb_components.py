"""
Tests for MariaDB ETL components (unit-level, with mocked receivers).

Updated: credentials are referenced via a persisted Credentials-Mapping Context,
and tests assert the active credentials via _get_credentials()["__credentials_id__"].
"""

from __future__ import annotations

from typing import Tuple
from unittest.mock import AsyncMock, Mock, patch

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.components.databases.mariadb.mariadb_read import MariaDBRead
from etl_core.components.databases.mariadb.mariadb_write import MariaDBWrite
from etl_core.components.wiring.column_definition import DataType, FieldDef
from etl_core.components.wiring.schema import Schema
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.context.credentials import Credentials


class TestMariaDBComponents:
    """Test cases for MariaDB components."""

    def _create_mariadb_write_with_schema(self, **kwargs) -> MariaDBWrite:
        """Helper to create MariaDBWrite component with proper schema."""
        mock_schema = Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )
        if "in_port_schemas" not in kwargs:
            kwargs["in_port_schemas"] = {"in": mock_schema}
        return MariaDBWrite(**kwargs)

    @pytest.fixture
    def mock_metrics(self) -> ComponentMetrics:
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
    def sample_dataframe(self) -> pd.DataFrame:
        """Sample pandas DataFrame for testing."""
        return pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["John", "Jane"],
                "email": ["john@example.com", "jane@example.com"],
            }
        )

    @pytest.fixture
    def sample_dask_dataframe(self) -> dd.DataFrame:
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

    def test_mariadb_read_initialization(
        self,
        persisted_mapping_context_id: str,
        persisted_credentials: Credentials,
        test_creds: Tuple[str, str],
    ) -> None:
        """Test MariaDBRead component initialization with mapping context."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                params={"limit": 10},
                context_id=persisted_mapping_context_id,
            )

        assert read_comp.query == "SELECT * FROM users"
        assert read_comp.params == {"limit": 10}

        # New assertion: active credentials come from _get_credentials()
        active_id = read_comp._get_credentials()["__credentials_id__"]
        assert active_id == persisted_credentials.credentials_id

        # Optional sanity: the resolved object is present
        assert read_comp._credentials is not None
        assert (
            read_comp._credentials.credentials_id
            == persisted_credentials.credentials_id
        )

    def test_mariadb_write_initialization(
        self,
        persisted_mapping_context_id: str,
        persisted_credentials: Credentials,
    ) -> None:
        """Test MariaDBWrite component initialization with mapping context."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = self._create_mariadb_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                context_id=persisted_mapping_context_id,
            )

        assert write_comp.entity_name == "users"
        active_id = write_comp._get_credentials()["__credentials_id__"]
        assert active_id == persisted_credentials.credentials_id

    @pytest.mark.asyncio
    async def test_mariadb_read_process_row(
        self,
        persisted_mapping_context_id: str,
        mock_metrics: ComponentMetrics,
        sample_data,
    ) -> None:
        """Test MariaDBRead process_row method."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users WHERE id = %(id)s",
                params={"id": 1},
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            for item in sample_data:
                yield item

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        results = []
        async for result in read_comp.process_row({"id": 1}, mock_metrics):
            results.append(result.payload)

        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[1]["id"] == 2

    @pytest.mark.asyncio
    async def test_mariadb_read_process_bulk(
        self,
        persisted_mapping_context_id: str,
        mock_metrics: ComponentMetrics,
        sample_dataframe: pd.DataFrame,
    ) -> None:
        """Test MariaDBRead process_bulk method."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = sample_dataframe
        read_comp._receiver = mock_receiver

        results = []
        async for result in read_comp.process_bulk(sample_dataframe, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1
        assert len(results[0]) == 2

    @pytest.mark.asyncio
    async def test_mariadb_read_process_bigdata(
        self,
        persisted_mapping_context_id: str,
        mock_metrics: ComponentMetrics,
        sample_dask_dataframe: dd.DataFrame,
    ) -> None:
        """Test MariaDBRead process_bigdata method."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_bigdata.return_value = sample_dask_dataframe
        read_comp._receiver = mock_receiver

        results = []
        async for result in read_comp.process_bigdata(
            sample_dask_dataframe, mock_metrics
        ):
            results.append(result.payload)

        assert len(results) == 1
        assert hasattr(results[0], "npartitions")

    @pytest.mark.asyncio
    async def test_mariadb_write_process_row(
        self,
        persisted_mapping_context_id: str,
        mock_metrics: ComponentMetrics,
    ) -> None:
        """Test MariaDBWrite process_row method."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = self._create_mariadb_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = {
            "affected_rows": 1,
            "row": {"name": "John", "email": "john@example.com"},
        }
        write_comp._receiver = mock_receiver

        results = []
        async for result in write_comp.process_row(
            {"name": "John", "email": "john@example.com"}, mock_metrics
        ):
            results.append(result.payload)

        assert len(results) == 1
        assert results[0]["affected_rows"] == 1
        assert results[0]["row"]["name"] == "John"

    @pytest.mark.asyncio
    async def test_mariadb_write_process_bulk(
        self,
        persisted_mapping_context_id: str,
        mock_metrics: ComponentMetrics,
        sample_dataframe: pd.DataFrame,
    ) -> None:
        """Test MariaDBWrite process_bulk method."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = self._create_mariadb_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.write_bulk.return_value = sample_dataframe
        write_comp._receiver = mock_receiver

        results = []
        async for result in write_comp.process_bulk(sample_dataframe, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1
        assert len(results[0]) == 2

    @pytest.mark.asyncio
    async def test_mariadb_write_process_bigdata(
        self,
        persisted_mapping_context_id: str,
        mock_metrics: ComponentMetrics,
        sample_dask_dataframe: dd.DataFrame,
    ) -> None:
        """Test MariaDBWrite process_bigdata method."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = self._create_mariadb_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                context_id=persisted_mapping_context_id,
                if_exists="replace",
                bigdata_partition_chunk_size=25_000,
            )

        mock_receiver = AsyncMock()
        mock_receiver.write_bigdata.return_value = sample_dask_dataframe
        write_comp._receiver = mock_receiver

        results = []
        async for result in write_comp.process_bigdata(
            sample_dask_dataframe, mock_metrics
        ):
            results.append(result.payload)

        mock_receiver.write_bigdata.assert_called_once()
        call_args = mock_receiver.write_bigdata.call_args
        assert "query" in call_args.kwargs
        assert call_args.kwargs["entity_name"] == "users"
        assert call_args.kwargs["frame"] is sample_dask_dataframe
        assert call_args.kwargs["metrics"] == mock_metrics
        assert call_args.kwargs["connection_handler"] == write_comp.connection_handler

        assert len(results) == 1
        assert hasattr(results[0], "npartitions")

    def test_mariadb_component_connection_setup(
        self, persisted_mapping_context_id: str
    ) -> None:
        """Test MariaDB component connection setup (no real connection)."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                context_id=persisted_mapping_context_id,
            )

        assert read_comp.name == "test_read"
        assert read_comp.comp_type == "read_mariadb"
        assert read_comp.entity_name == "users"
        assert read_comp.query == "SELECT * FROM users"
        assert hasattr(read_comp, "_connection_handler")
        assert hasattr(read_comp, "_receiver")

    @pytest.mark.asyncio
    async def test_mariadb_component_error_handling(
        self, persisted_mapping_context_id: str, mock_metrics: ComponentMetrics
    ) -> None:
        """Test MariaDB component error handling."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_row.side_effect = Exception("Database error")
        read_comp._receiver = mock_receiver

        with pytest.raises(Exception):
            async for _ in read_comp.process_row({"id": 1}, mock_metrics):
                pass

    @pytest.mark.asyncio
    async def test_mariadb_component_strategy_integration(
        self, persisted_mapping_context_id: str, mock_metrics: ComponentMetrics
    ) -> None:
        """Test MariaDB component strategy integration."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            yield {"id": 1, "name": "John"}

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        class _DummyStrategy:
            async def execute(self, component, payload, metrics):
                async for item in component.process_row(payload, metrics):
                    yield item

        read_comp._strategy = _DummyStrategy()  # type: ignore[attr-defined]

        payload = {"id": 1}
        results = []
        async for result in read_comp.execute(payload, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1
        assert results[0]["id"] == 1

    def test_mariadb_write_batch_size_configuration(
        self, persisted_mapping_context_id: str
    ) -> None:
        """Test MariaDBWrite batch size configuration."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = self._create_mariadb_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                context_id=persisted_mapping_context_id,
                row_batch_size=500,
            )

        assert write_comp.row_batch_size == 500

    @pytest.mark.asyncio
    async def test_mariadb_read_with_complex_params(
        self, persisted_mapping_context_id: str, mock_metrics: ComponentMetrics
    ) -> None:
        """Test MariaDBRead with complex query parameters."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query=(
                    "SELECT * FROM users "
                    "WHERE age > %(min_age)s AND city = ANY(%(cities)s)"
                ),
                params={"min_age": 18, "cities": ["Berlin", "München", "Hamburg"]},
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            yield {"id": 1, "name": "John", "age": 25, "city": "Berlin"}

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        results = []
        async for result in read_comp.process_row(
            {"min_age": 18, "cities": ["Berlin"]}, mock_metrics
        ):
            results.append(result.payload)

        assert len(results) == 1
        assert results[0]["city"] == "Berlin"

    @pytest.mark.asyncio
    async def test_mariadb_write_with_empty_data(
        self, persisted_mapping_context_id: str, mock_metrics: ComponentMetrics
    ) -> None:
        """Test MariaDBWrite with empty data."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = self._create_mariadb_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                context_id=persisted_mapping_context_id,
            )

        empty_df = pd.DataFrame()
        mock_receiver = AsyncMock()
        mock_receiver.write_bulk.return_value = empty_df
        write_comp._receiver = mock_receiver

        results = []
        async for result in write_comp.process_bulk(empty_df, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1
        assert isinstance(results[0], pd.DataFrame)
        assert results[0].empty
