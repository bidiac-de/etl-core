"""
Unit tests for PostgreSQL ETL components (mocked receivers/handler).

All credential resolution is direct via credentials_id (no Context usage).
"""

from __future__ import annotations

import os
from typing import Tuple
from uuid import uuid4
from unittest.mock import AsyncMock, Mock, patch

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.components.databases.if_exists_strategy import DatabaseOperation
from etl_core.components.databases.postgresql.postgresql_read import PostgreSQLRead
from etl_core.components.databases.postgresql.postgresql_write import PostgreSQLWrite
from etl_core.components.wiring.column_definition import DataType, FieldDef
from etl_core.components.wiring.schema import Schema
from etl_core.context.credentials import Credentials
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.persistance.handlers.credentials_handler import CredentialsHandler
from etl_core.strategies.base_strategy import ExecutionStrategy


class TestPostgreSQLComponents:
    """Test cases for PostgreSQL components."""

    def _create_pg_write_with_schema(self, **kwargs) -> PostgreSQLWrite:
        schema = Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )
        if "in_port_schemas" not in kwargs:
            kwargs["in_port_schemas"] = {"in": schema}
        return PostgreSQLWrite(**kwargs)

    @pytest.fixture
    def test_creds(self) -> Tuple[str, str]:
        return os.environ["APP_TEST_USER"], os.environ["APP_TEST_PASSWORD"]

    @pytest.fixture
    def persisted_credentials(self, test_creds: Tuple[str, str]) -> Credentials:
        user, password = test_creds
        creds = Credentials(
            credentials_id=str(uuid4()),
            name="pg_test_creds",
            user=user,
            host="localhost",
            port=5432,
            database="testdb",
            password=password,
            pool_max_size=10,
            pool_timeout_s=30,
        )
        CredentialsHandler().upsert(provider_id=str(uuid4()), creds=creds)
        return creds

    @pytest.fixture
    def mock_metrics(self) -> ComponentMetrics:
        m = Mock(spec=ComponentMetrics)
        m.set_started = Mock()
        m.set_completed = Mock()
        m.set_failed = Mock()
        return m

    @pytest.fixture
    def sample_data(self):
        return [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"},
        ]

    @pytest.fixture
    def sample_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["John", "Jane"],
                "email": ["john@example.com", "jane@example.com"],
            }
        )

    @pytest.fixture
    def sample_dask_dataframe(self) -> dd.DataFrame:
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

    def test_postgresql_read_initialization(
        self, persisted_credentials: Credentials
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = PostgreSQLRead(
                name="test_read",
                description="Test read component",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT * FROM users",
                params={"limit": 10},
                credentials_id=persisted_credentials.credentials_id,
            )
        assert comp.query == "SELECT * FROM users"
        assert comp.params == {"limit": 10}
        assert comp.credentials_id == persisted_credentials.credentials_id

    def test_postgresql_write_initialization(
        self, persisted_credentials: Credentials
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = self._create_pg_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_postgresql",
                entity_name="users",
                credentials_id=persisted_credentials.credentials_id,
            )
        assert comp.entity_name == "users"
        assert comp.credentials_id == persisted_credentials.credentials_id

    @pytest.mark.asyncio
    async def test_postgresql_read_process_row(
        self,
        persisted_credentials: Credentials,
        mock_metrics: ComponentMetrics,
        sample_data,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = PostgreSQLRead(
                name="test_read",
                description="Test read",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT * FROM users WHERE id = %(id)s",
                params={"id": 1},
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()

        async def gen(entity_name, metrics, **driver_kwargs):
            for item in sample_data:
                yield item

        mock_receiver.read_row = gen
        comp._receiver = mock_receiver

        results = []
        async for out in comp.process_row({"id": 1}, mock_metrics):
            results.append(out.payload)

        assert [r["id"] for r in results] == [1, 2]

    @pytest.mark.asyncio
    async def test_postgresql_read_process_bulk(
        self,
        persisted_credentials: Credentials,
        mock_metrics: ComponentMetrics,
        sample_dataframe: pd.DataFrame,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = PostgreSQLRead(
                name="test_read",
                description="Test read",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = sample_dataframe
        comp._receiver = mock_receiver

        outs = []
        async for out in comp.process_bulk(sample_dataframe, mock_metrics):
            outs.append(out.payload)

        assert len(outs) == 1 and len(outs[0]) == 2

    @pytest.mark.asyncio
    async def test_postgresql_read_process_bigdata(
        self,
        persisted_credentials: Credentials,
        mock_metrics: ComponentMetrics,
        sample_dask_dataframe: dd.DataFrame,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = PostgreSQLRead(
                name="test_read",
                description="Test read",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_bigdata.return_value = sample_dask_dataframe
        comp._receiver = mock_receiver

        outs = []
        async for out in comp.process_bigdata(sample_dask_dataframe, mock_metrics):
            outs.append(out.payload)

        assert len(outs) == 1 and hasattr(outs[0], "npartitions")

    @pytest.mark.asyncio
    async def test_postgresql_write_process_row(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = self._create_pg_write_with_schema(
                name="test_write",
                description="Test write",
                comp_type="write_postgresql",
                entity_name="users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = {
            "affected_rows": 1,
            "row": {"name": "John"},
        }
        comp._receiver = mock_receiver

        outs = []
        async for out in comp.process_row({"name": "John"}, mock_metrics):
            outs.append(out.payload)

        assert len(outs) == 1 and outs[0]["affected_rows"] == 1

    @pytest.mark.asyncio
    async def test_postgresql_write_process_bulk(
        self,
        persisted_credentials: Credentials,
        mock_metrics: ComponentMetrics,
        sample_dataframe: pd.DataFrame,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = self._create_pg_write_with_schema(
                name="test_write",
                description="Test write",
                comp_type="write_postgresql",
                entity_name="users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.write_bulk.return_value = sample_dataframe
        comp._receiver = mock_receiver

        outs = []
        async for out in comp.process_bulk(sample_dataframe, mock_metrics):
            outs.append(out.payload)

        assert len(outs) == 1 and len(outs[0]) == 2

    @pytest.mark.asyncio
    async def test_postgresql_write_process_bigdata(
        self,
        persisted_credentials: Credentials,
        mock_metrics: ComponentMetrics,
        sample_dask_dataframe: dd.DataFrame,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = self._create_pg_write_with_schema(
                name="test_write",
                description="Test write",
                comp_type="write_postgresql",
                entity_name="users",
                if_exists="replace",
                bigdata_partition_chunk_size=25_000,
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.write_bigdata.return_value = sample_dask_dataframe
        comp._receiver = mock_receiver

        outs = []
        async for out in comp.process_bigdata(sample_dask_dataframe, mock_metrics):
            outs.append(out.payload)

        mock_receiver.write_bigdata.assert_called_once()
        kwargs = mock_receiver.write_bigdata.call_args.kwargs
        assert kwargs["entity_name"] == "users"
        assert kwargs["frame"] is sample_dask_dataframe
        assert kwargs["metrics"] == mock_metrics
        assert kwargs["connection_handler"] == comp.connection_handler
        assert len(outs) == 1 and hasattr(outs[0], "npartitions")

    def test_postgresql_component_connection_setup(
        self, persisted_credentials: Credentials
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = PostgreSQLRead(
                name="test_read",
                description="Test",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )
        assert comp.name == "test_read"
        assert comp.comp_type == "read_postgresql"
        assert comp.entity_name == "users"
        assert comp.query == "SELECT * FROM users"
        assert hasattr(comp, "_connection_handler")
        assert hasattr(comp, "_receiver")

    @pytest.mark.asyncio
    async def test_postgresql_component_error_handling(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = PostgreSQLRead(
                name="test_read",
                description="Test",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_row.side_effect = Exception("Database error")
        comp._receiver = mock_receiver

        with pytest.raises(Exception):
            async for _ in comp.process_row({"id": 1}, mock_metrics):
                pass

    @pytest.mark.asyncio
    async def test_postgresql_component_strategy_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = PostgreSQLRead(
                name="test_read",
                description="Test",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()

        async def gen(entity_name, metrics, **driver_kwargs):
            yield {"id": 1, "name": "John"}

        mock_receiver.read_row = gen
        comp._receiver = mock_receiver

        strategy = Mock(spec=ExecutionStrategy)

        async def exec_gen(component, payload, metrics):
            async for item in comp.process_row(payload, metrics):
                yield item

        strategy.execute = exec_gen
        comp._strategy = strategy

        results = []
        async for out in comp.execute({"id": 1}, mock_metrics):
            results.append(out.payload)

        assert len(results) == 1 and results[0]["id"] == 1

    def test_postgresql_write_batch_size_configuration(
        self, persisted_credentials: Credentials
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = self._create_pg_write_with_schema(
                name="test_write",
                description="Test",
                comp_type="write_postgresql",
                entity_name="users",
                row_batch_size=500,
                credentials_id=persisted_credentials.credentials_id,
            )
        assert comp.row_batch_size == 500

    @pytest.mark.asyncio
    async def test_postgresql_read_with_complex_params(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = PostgreSQLRead(
                name="test_read",
                description="Test",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT * FROM users WHERE age > %(min_age)s"
                " AND city = ANY(%(cities)s)",
                params={"min_age": 18, "cities": ["Berlin", "München", "Hamburg"]},
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()

        async def gen(entity_name, metrics, **driver_kwargs):
            yield {"id": 1, "name": "John", "age": 25, "city": "Berlin"}

        mock_receiver.read_row = gen
        comp._receiver = mock_receiver

        outs = []
        async for out in comp.process_row(
            {"min_age": 18, "cities": ["Berlin"]}, mock_metrics
        ):
            outs.append(out.payload)

        assert len(outs) == 1 and outs[0]["city"] == "Berlin"

    @pytest.mark.asyncio
    async def test_postgresql_write_with_empty_data(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = self._create_pg_write_with_schema(
                name="test_write",
                description="Test",
                comp_type="write_postgresql",
                entity_name="users",
                credentials_id=persisted_credentials.credentials_id,
            )

        empty_df = pd.DataFrame()
        mock_receiver = AsyncMock()
        mock_receiver.write_bulk.return_value = empty_df
        comp._receiver = mock_receiver

        outs = []
        async for out in comp.process_bulk(empty_df, mock_metrics):
            outs.append(out.payload)

        assert len(outs) == 1 and len(outs[0]) == 0

    def test_invalid_credentials_id_raises(self) -> None:
        """Unknown credentials_id should raise when creds are actually resolved."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            bogus = "no-such-id"
            comp = PostgreSQLRead(
                name="bad",
                description="",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT 1",
                credentials_id=bogus,
            )
        with pytest.raises(
            ValueError, match="Credentials with ID no-such-id not found"
        ):
            _ = comp._get_credentials()

    @pytest.mark.asyncio
    async def test_postgresql_component_metrics_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = PostgreSQLRead(
                name="test_read",
                description="Test",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()

        async def gen(entity_name, metrics, **driver_kwargs):
            mock_metrics.set_started()
            yield {"id": 1, "name": "John"}
            mock_metrics.set_completed()

        mock_receiver.read_row = gen
        comp._receiver = mock_receiver

        outs = []
        async for out in comp.process_row({"id": 1}, mock_metrics):
            outs.append(out.payload)

        mock_metrics.set_started.assert_called_once()
        mock_metrics.set_completed.assert_called_once()
        assert len(outs) == 1

    def test_postgresql_component_port_configuration(
        self, persisted_credentials: Credentials
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = PostgreSQLRead(
                name="test_read",
                description="Test read",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )
        assert len(read_comp.INPUT_PORTS) == 0
        assert read_comp.ALLOW_NO_INPUTS is True
        assert len(read_comp.OUTPUT_PORTS) == 1
        assert read_comp.OUTPUT_PORTS[0].name == "out"
        assert read_comp.OUTPUT_PORTS[0].required is True
        assert read_comp.OUTPUT_PORTS[0].fanout == "many"

        schema = Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            write_comp = PostgreSQLWrite(
                name="test_write",
                description="Test write",
                comp_type="write_postgresql",
                entity_name="users",
                in_port_schemas={"in": schema},
                credentials_id=persisted_credentials.credentials_id,
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
    async def test_postgresql_component_query_building(
        self, persisted_credentials: Credentials
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = self._create_pg_write_with_schema(
                name="test_write",
                description="Test",
                comp_type="write_postgresql",
                entity_name="users",
                operation="insert",
                credentials_id=persisted_credentials.credentials_id,
            )

        cols = ["id", "name", "email"]
        q_ins = comp._build_query("users", cols, DatabaseOperation.INSERT)
        assert "INSERT INTO users" in q_ins and "id, name, email" in q_ins

        q_up = comp._build_query(
            "users", cols, DatabaseOperation.UPSERT, conflict_columns=["id"]
        )
        assert "INSERT INTO users" in q_up and "ON CONFLICT" in q_up

        comp.where_conditions = ["id = :id"]
        q_upd = comp._build_query("users", cols, DatabaseOperation.UPDATE)
        assert "UPDATE users" in q_upd and "WHERE" in q_upd

        q_tr = comp._build_query("users", cols, DatabaseOperation.TRUNCATE)
        assert "TRUNCATE TABLE users" in q_tr

    @pytest.mark.asyncio
    async def test_postgresql_component_receiver_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = self._create_pg_write_with_schema(
                name="test_write",
                description="Test",
                comp_type="write_postgresql",
                entity_name="users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = {"affected_rows": 1}
        mock_receiver.write_bulk.return_value = pd.DataFrame({"id": [1]})
        mock_receiver.write_bigdata.return_value = dd.from_pandas(
            pd.DataFrame({"id": [1]}), npartitions=1
        )
        comp._receiver = mock_receiver

        row = {"id": 1, "name": "John"}
        df = pd.DataFrame([row])
        ddf = dd.from_pandas(df, npartitions=1)

        outs = []
        async for out in comp.process_row(row, mock_metrics):
            outs.append(out.payload)
        assert len(outs) == 1 and mock_receiver.write_row.called

        outs = []
        async for out in comp.process_bulk(df, mock_metrics):
            outs.append(out.payload)
        assert len(outs) == 1 and mock_receiver.write_bulk.called

        outs = []
        async for out in comp.process_bigdata(ddf, mock_metrics):
            outs.append(out.payload)
        assert len(outs) == 1 and mock_receiver.write_bigdata.called

    @pytest.mark.asyncio
    async def test_postgresql_component_connection_handler_integration(
        self, persisted_credentials: Credentials
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = self._create_pg_write_with_schema(
                name="test_write",
                description="Test",
                comp_type="write_postgresql",
                entity_name="users",
                credentials_id=persisted_credentials.credentials_id,
            )

        assert hasattr(comp, "_connection_handler")
        comp._connection_handler = Mock()
        comp._build_objects()
        assert comp.connection_handler is not None

        mock_receiver = AsyncMock()
        mock_receiver.write_row.return_value = {"affected_rows": 1}
        comp._receiver = mock_receiver

        outs = []
        async for out in comp.process_row({"id": 1, "name": "John"}, Mock()):
            outs.append(out.payload)
        kwargs = mock_receiver.write_row.call_args.kwargs
        assert kwargs["connection_handler"] == comp.connection_handler
