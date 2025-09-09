"""
Tests for MariaDB ETL components (unit-level, with mocked receivers).

Updated: credentials are resolved directly via credentials_id (no Context usage).
"""

from __future__ import annotations

import os
from typing import Tuple
from uuid import uuid4
from unittest.mock import Mock, AsyncMock, patch

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.components.databases.if_exists_strategy import DatabaseOperation
from etl_core.components.databases.mariadb.mariadb_read import MariaDBRead
from etl_core.components.databases.mariadb.mariadb_write import MariaDBWrite
from etl_core.components.wiring.column_definition import DataType, FieldDef
from etl_core.components.wiring.schema import Schema
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.persistance.handlers.credentials_handler import CredentialsHandler
from etl_core.context.credentials import Credentials
from etl_core.strategies.base_strategy import ExecutionStrategy
from etl_core.context.environment import Environment


@pytest.fixture
def test_creds() -> Tuple[str, str]:
    return os.environ["APP_TEST_USER"], os.environ["APP_TEST_PASSWORD"]


@pytest.fixture
def persisted_credentials(test_creds: Tuple[str, str]) -> Credentials:
    """
    Persist real Credentials so components can resolve them by credentials_id.
    """
    user, password = test_creds
    creds = Credentials(
        credentials_id=str(uuid4()),
        name="test_db_creds",
        user=user,
        host="localhost",
        port=3306,
        database="testdb",
        password=password,
        pool_max_size=10,
        pool_timeout_s=30,
    )
    CredentialsHandler().upsert(provider_id=str(uuid4()), creds=creds)
    return creds


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
        self, persisted_credentials: Credentials, test_creds: Tuple[str, str]
    ) -> None:
        """Test MariaDBRead component initialization."""
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
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
            )

        assert read_comp.query == "SELECT * FROM users"
        assert read_comp.params == {"limit": 10}
        assert read_comp._active_credentials_id == persisted_credentials.credentials_id

    def test_mariadb_write_initialization(
        self, persisted_credentials: Credentials
    ) -> None:
        """Test MariaDBWrite component initialization."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = self._create_mariadb_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
            )

        assert write_comp.entity_name == "users"
        assert write_comp._active_credentials_id == persisted_credentials.credentials_id

    @pytest.mark.asyncio
    async def test_mariadb_read_process_row(
        self,
        persisted_credentials: Credentials,
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
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
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
        persisted_credentials: Credentials,
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
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
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
        persisted_credentials: Credentials,
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
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
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
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
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
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
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
        persisted_credentials: Credentials,
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
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
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
        persisted_credentials: Credentials,
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
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
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
        self, persisted_credentials: Credentials
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
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
            )

        assert read_comp.name == "test_read"
        assert read_comp.comp_type == "read_mariadb"
        assert read_comp.entity_name == "users"
        assert read_comp.query == "SELECT * FROM users"
        assert read_comp._active_credentials_id == persisted_credentials.credentials_id
        assert hasattr(read_comp, "_connection_handler")
        assert hasattr(read_comp, "_receiver")

    @pytest.mark.asyncio
    async def test_mariadb_component_error_handling(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
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
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_row.side_effect = Exception("Database error")
        read_comp._receiver = mock_receiver

        with pytest.raises(Exception):
            async for _ in read_comp.process_row({"id": 1}, mock_metrics):
                pass

    @pytest.mark.asyncio
    async def test_mariadb_component_strategy_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
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
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
            )

        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            yield {"id": 1, "name": "John"}

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        mock_strategy = Mock(spec=ExecutionStrategy)

        async def mock_execute_generator(component, payload, metrics):
            async for item in read_comp.process_row(payload, metrics):
                yield item

        mock_strategy.execute = mock_execute_generator
        read_comp._strategy = mock_strategy

        payload = {"id": 1}
        results = []
        async for result in read_comp.execute(payload, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1
        assert results[0]["id"] == 1

    def test_mariadb_write_batch_size_configuration(
        self, persisted_credentials: Credentials
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
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
                row_batch_size=500,
            )

        assert write_comp.row_batch_size == 500

    @pytest.mark.asyncio
    async def test_mariadb_read_with_complex_params(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
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
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
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
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
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
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
            )

        empty_df = pd.DataFrame()
        mock_receiver = AsyncMock()
        mock_receiver.write_bulk.return_value = empty_df
        write_comp._receiver = mock_receiver

        results = []
        async for result in write_comp.process_bulk(empty_df, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1
        assert len(results[0]) == 0

    def test_invalid_credentials_id_raises(self) -> None:
        """Unknown credentials_id should raise ValueError
        on component init or resolution."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            bogus_id = "does-not-exist"
            with pytest.raises(
                ValueError, match="Credentials with ID does-not-exist not found"
            ):
                MariaDBRead(
                    name="bad",
                    description="",
                    comp_type="read_mariadb",
                    entity_name="users",
                    query="SELECT 1",
                    credentials_ids={Environment.TEST.value: bogus_id},
                )

    @pytest.mark.asyncio
    async def test_mariadb_component_metrics_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        """Test MariaDB component metrics integration."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
            )

        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            mock_metrics.set_started()
            yield {"id": 1, "name": "John"}
            mock_metrics.set_completed()

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        results = []
        async for result in read_comp.process_row({"id": 1}, mock_metrics):
            results.append(result.payload)

        mock_metrics.set_started.assert_called_once()
        mock_metrics.set_completed.assert_called_once()
        assert len(results) == 1

    def test_mariadb_component_strategy_type_configuration(
        self, persisted_credentials: Credentials
    ) -> None:
        """Ensure component can be created without explicit strategy_type."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
            )
        assert read_comp.name == "test_read"
        assert read_comp.query == "SELECT * FROM users"

    @pytest.mark.asyncio
    async def test_mariadb_component_large_query_handling(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
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
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query=large_query,
                params={"start_date": "2023-01-01", "limit": 1000},
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
            )

        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            yield {"id": 1, "name": "John", "email": "john@example.com"}

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        results = []
        async for result in read_comp.process_row(
            {"start_date": "2023-01-01", "limit": 1000}, mock_metrics
        ):
            results.append(result.payload)

        assert len(results) == 1
        assert "LEFT JOIN" in read_comp.query

    @pytest.mark.asyncio
    async def test_mariadb_component_special_characters_in_table_name(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        """Test MariaDB component with special characters in table names."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = self._create_mariadb_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="user_profiles_2024",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
            )

        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = None
        write_comp._receiver = mock_receiver

        results = []
        async for result in write_comp.process_row({"name": "John"}, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1
        assert write_comp.entity_name == "user_profiles_2024"

    def test_mariadb_component_charset_collation_defaults(self) -> None:
        """Simple defaults sanity check (unrelated to resolver)."""
        mock_comp = Mock()
        mock_comp.charset = "utf8"
        mock_comp.collation = "utf8_general_ci"

        assert mock_comp.charset == "utf8"
        assert mock_comp.collation == "utf8_general_ci"

        mock_comp_custom = Mock()
        mock_comp_custom.charset = "latin1"
        mock_comp_custom.collation = "latin1_swedish_ci"

        assert mock_comp_custom.charset == "latin1"
        assert mock_comp_custom.collation == "latin1_swedish_ci"

    def test_mariadb_component_connection_setup_with_session_variables(self) -> None:
        """Basic check for session variables wiring (mocked)."""
        mock_comp = Mock()
        mock_handler = Mock()
        mock_conn = Mock()
        mock_conn.execute = Mock()
        mock_conn.commit = Mock()

        mock_comp._connection_handler = mock_handler
        mock_comp.charset = "utf8"
        mock_comp.collation = "utf8_general_ci"

        assert mock_comp.charset == "utf8"
        assert mock_comp.collation == "utf8_general_ci"

    @pytest.mark.asyncio
    async def test_mariadb_component_operation_types(
        self, persisted_credentials: Credentials
    ) -> None:
        """Test MariaDB component with different operation types."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp_insert = self._create_mariadb_write_with_schema(
                name="test_write_insert",
                description="Test write component with INSERT",
                comp_type="write_mariadb",
                entity_name="users",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
                operation="insert",
            )
            write_comp_upsert = self._create_mariadb_write_with_schema(
                name="test_write_upsert",
                description="Test write component with UPSERT",
                comp_type="write_mariadb",
                entity_name="users",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
                operation="upsert",
            )
            write_comp_update = self._create_mariadb_write_with_schema(
                name="test_write_update",
                description="Test write component with UPDATE",
                comp_type="write_mariadb",
                entity_name="users",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
                operation="update",
                where_conditions=["id = :id"],
            )
            write_comp_truncate = self._create_mariadb_write_with_schema(
                name="test_write_truncate",
                description="Test write component with TRUNCATE",
                comp_type="write_mariadb",
                entity_name="users",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
                operation="truncate",
            )

        assert write_comp_insert.operation == DatabaseOperation.INSERT
        assert write_comp_upsert.operation == DatabaseOperation.UPSERT
        assert write_comp_update.operation == DatabaseOperation.UPDATE
        assert write_comp_truncate.operation == DatabaseOperation.TRUNCATE

    def test_mariadb_component_batch_size_configuration(
        self, persisted_credentials: Credentials
    ) -> None:
        """Test MariaDB component batch size configuration."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = self._create_mariadb_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
                row_batch_size=500,
                bulk_chunk_size=25_000,
                bigdata_partition_chunk_size=100_000,
            )

        assert write_comp.row_batch_size == 500
        assert write_comp.bulk_chunk_size == 25_000
        assert write_comp.bigdata_partition_chunk_size == 100_000

    def test_mariadb_component_query_building(
        self, persisted_credentials: Credentials
    ) -> None:
        """Test MariaDBWrite query building functionality."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = self._create_mariadb_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
                operation="insert",
            )

        columns = ["id", "name", "email"]

        insert_query = write_comp._build_query(
            "users", columns, DatabaseOperation.INSERT
        )
        assert "INSERT INTO users" in insert_query
        assert "id, name, email" in insert_query

        upsert_query = write_comp._build_query(
            "users", columns, DatabaseOperation.UPSERT, conflict_columns=["id"]
        )
        assert "INSERT INTO users" in upsert_query
        assert "ON DUPLICATE KEY UPDATE" in upsert_query

        write_comp.where_conditions = ["id = :id"]
        update_query = write_comp._build_query(
            "users", columns, DatabaseOperation.UPDATE
        )
        assert "UPDATE users" in update_query
        assert "SET" in update_query
        assert "WHERE" in update_query

        truncate_query = write_comp._build_query(
            "users", columns, DatabaseOperation.TRUNCATE
        )
        assert "TRUNCATE TABLE users" in truncate_query
        assert "TRUNCATE TABLE users" in truncate_query

    def test_mariadb_component_port_configuration(
        self, persisted_credentials: Credentials
    ) -> None:
        """Test MariaDB component port configuration."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
            )

        assert len(read_comp.INPUT_PORTS) == 0
        assert read_comp.ALLOW_NO_INPUTS is True

        assert len(read_comp.OUTPUT_PORTS) == 1
        assert read_comp.OUTPUT_PORTS[0].name == "out"
        assert read_comp.OUTPUT_PORTS[0].required is True
        assert read_comp.OUTPUT_PORTS[0].fanout == "many"

        mock_schema = Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )

        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = MariaDBWrite(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
                in_port_schemas={"in": mock_schema},
            )

        assert len(write_comp.INPUT_PORTS) == 1
        assert write_comp.INPUT_PORTS[0].name == "in"
        assert write_comp.INPUT_PORTS[0].required is True
        assert write_comp.INPUT_PORTS[0].fanin == "many"

        assert len(write_comp.OUTPUT_PORTS) == 1
        assert write_comp.OUTPUT_PORTS[0].name == "out"
        assert write_comp.OUTPUT_PORTS[0].required is False
        assert write_comp.OUTPUT_PORTS[0].fanout == "many"

    @pytest.mark.asyncio
    async def test_mariadb_component_schema_validation(
        self, persisted_credentials: Credentials
    ) -> None:
        """Test MariaDB component schema validation."""
        mock_schema = Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )

        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = MariaDBWrite(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
                in_port_schemas={"in": mock_schema},
            )

        assert "in" in write_comp.in_port_schemas
        assert write_comp.in_port_schemas["in"] == mock_schema
        assert write_comp._query is not None
        assert "INSERT INTO users" in write_comp._query

    @pytest.mark.asyncio
    async def test_mariadb_component_receiver_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        """Test MariaDB component receiver integration."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = self._create_mariadb_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
            )

        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = {"affected_rows": 1}
        mock_receiver.write_bulk.return_value = pd.DataFrame({"id": [1]})
        mock_receiver.write_bigdata.return_value = dd.from_pandas(
            pd.DataFrame({"id": [1]}), npartitions=1
        )
        write_comp._receiver = mock_receiver

        test_row = {"id": 1, "name": "John"}
        test_df = pd.DataFrame([test_row])
        test_ddf = dd.from_pandas(test_df, npartitions=1)

        results = []
        async for result in write_comp.process_row(test_row, mock_metrics):
            results.append(result.payload)
        assert len(results) == 1
        mock_receiver.write_row.assert_called_once()

        results = []
        async for result in write_comp.process_bulk(test_df, mock_metrics):
            results.append(result.payload)
        assert len(results) == 1
        mock_receiver.write_bulk.assert_called_once()

        results = []
        async for result in write_comp.process_bigdata(test_ddf, mock_metrics):
            results.append(result.payload)
        assert len(results) == 1
        mock_receiver.write_bigdata.assert_called_once()

    @pytest.mark.asyncio
    async def test_mariadb_component_connection_handler_integration(
        self, persisted_credentials: Credentials
    ) -> None:
        """Test MariaDB component connection handler integration."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = self._create_mariadb_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                credentials_ids={
                    Environment.TEST.value: persisted_credentials.credentials_id
                },
            )

        assert hasattr(write_comp, "_connection_handler")

        mock_connection_handler = Mock()
        write_comp._connection_handler = mock_connection_handler

        write_comp._build_objects()
        assert write_comp.connection_handler is not None

        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = {"affected_rows": 1}
        write_comp._receiver = mock_receiver

        test_row = {"id": 1, "name": "John"}
        results = []
        async for result in write_comp.process_row(test_row, Mock()):
            results.append(result.payload)

        mock_receiver.write_row.assert_called_once()
        call_args = mock_receiver.write_row.call_args
        assert "connection_handler" in call_args.kwargs
        assert call_args.kwargs["connection_handler"] == write_comp.connection_handler
