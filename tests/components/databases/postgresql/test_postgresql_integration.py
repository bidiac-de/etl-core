"""
Integration-style tests for PostgreSQL components (mocked receivers/handler).

All credentials are resolved via a persisted mapping context (context_id on comps).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock, patch

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.components.databases.postgresql.postgresql_read import PostgreSQLRead
from etl_core.components.databases.postgresql.postgresql_write import PostgreSQLWrite
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.strategies.base_strategy import ExecutionStrategy


class TestPostgreSQLIntegration:
    """Test PostgreSQL integration scenarios."""

    def _create_pg_write_with_schema(self, **kwargs) -> PostgreSQLWrite:
        from etl_core.components.wiring.schema import Schema
        from etl_core.components.wiring.column_definition import FieldDef, DataType

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
    def sample_ddf(self) -> dd.DataFrame:
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
        self,
        persisted_mapping_context_id: str,
        mock_metrics: ComponentMetrics,
        sample_data,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = PostgreSQLRead(
                name="test_read",
                description="Test",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT * FROM users",
                context_id=persisted_mapping_context_id,
            )
            write_comp = self._create_pg_write_with_schema(
                name="test_write",
                description="Test",
                comp_type="write_postgresql",
                entity_name="users",
                context_id=persisted_mapping_context_id,
            )

        # Sanity: both components resolved credentials via mapping
        assert read_comp._get_credentials()["__credentials_id__"]
        assert write_comp._get_credentials()["__credentials_id__"]

        mock_read_receiver = AsyncMock()

        async def read_gen(entity_name, metrics, **driver_kwargs):
            for item in sample_data:
                yield item

        mock_read_receiver.read_row = read_gen
        read_comp._receiver = mock_read_receiver

        mock_write_receiver = AsyncMock()
        mock_write_receiver.write_row.return_value = {
            "affected_rows": 1,
            "row": {"id": 1, "name": "John"},
        }
        write_comp._receiver = mock_write_receiver

        read_results = []
        async for out in read_comp.process_row({"id": 1}, mock_metrics):
            read_results.append(out.payload)
        assert len(read_results) == 2

        write_results = []
        async for out in write_comp.process_row(read_results[0], mock_metrics):
            write_results.append(out.payload)

        assert len(write_results) == 1
        assert write_results[0]["affected_rows"] == 1
        assert write_results[0]["row"]["id"] == 1

    @pytest.mark.asyncio
    async def test_row_strategy_streaming(
        self,
        persisted_mapping_context_id: str,
        mock_metrics: ComponentMetrics,
        sample_data,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = PostgreSQLRead(
                name="test_read",
                description="Test",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT * FROM users WHERE id = %(id)s",
                params={"id": 1},
                context_id=persisted_mapping_context_id,
            )

        mock_read_receiver = AsyncMock()

        async def gen(entity_name, metrics, **driver_kwargs):
            for item in sample_data:
                yield item

        mock_read_receiver.read_row = gen
        read_comp._receiver = mock_read_receiver

        strategy = Mock(spec=ExecutionStrategy)

        async def exec_gen(component, payload, metrics):
            async for item in read_comp.process_row(payload, metrics):
                yield item

        strategy.execute = exec_gen
        read_comp._strategy = strategy

        results = []
        async for out in read_comp.execute({"id": 1}, mock_metrics):
            results.append(out.payload)

        assert [r["id"] for r in results] == [1, 2]

    @pytest.mark.asyncio
    async def test_bigdata_strategy(
        self,
        persisted_mapping_context_id: str,
        mock_metrics: ComponentMetrics,
        sample_ddf: dd.DataFrame,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = PostgreSQLRead(
                name="test_read",
                description="Test",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT * FROM users",
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_bigdata.return_value = sample_ddf
        read_comp._receiver = mock_receiver

        strategy = Mock(spec=ExecutionStrategy)

        async def exec_gen(component, payload, metrics):
            gen = read_comp.process_bigdata(payload, metrics)
            out = await anext(gen)
            yield out

        strategy.execute = exec_gen
        read_comp._strategy = strategy

        outs = []
        async for out in read_comp.execute(sample_ddf, mock_metrics):
            outs.append(out.payload)

        assert len(outs) == 1 and hasattr(outs[0], "npartitions")

    @pytest.mark.asyncio
    async def test_error_propagation(
        self, persisted_mapping_context_id: str, mock_metrics: ComponentMetrics
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
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_row.side_effect = Exception("Database error")
        comp._receiver = mock_receiver

        with pytest.raises(Exception):
            async for _ in comp.process_row({"id": 1}, mock_metrics):
                pass

    @pytest.mark.asyncio
    async def test_metrics_integration(
        self, persisted_mapping_context_id: str, mock_metrics: ComponentMetrics
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
                context_id=persisted_mapping_context_id,
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

        mock_metrics.set_started.assert_called()
        mock_metrics.set_completed.assert_called()
        assert len(outs) == 1 and outs[0]["id"] == 1
