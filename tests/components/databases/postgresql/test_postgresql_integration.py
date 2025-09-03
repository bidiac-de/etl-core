"""
Integration-style tests for PostgreSQL components (mocked receivers/handler).

All credentials are resolved via credentials_id; no Context is used for creds.
"""

from __future__ import annotations

import asyncio
import os
from typing import Tuple
from uuid import uuid4
from unittest.mock import AsyncMock, Mock, patch

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.components.databases.postgresql.postgresql_read import PostgreSQLRead
from etl_core.components.databases.postgresql.postgresql_write import PostgreSQLWrite
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.persistance.handlers.credentials_handler import CredentialsHandler
from etl_core.context.credentials import Credentials
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
        persisted_credentials: Credentials,
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
                credentials_id=persisted_credentials.credentials_id,
            )
            write_comp = self._create_pg_write_with_schema(
                name="test_write",
                description="Test",
                comp_type="write_postgresql",
                entity_name="users",
                credentials_id=persisted_credentials.credentials_id,
            )

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
        persisted_credentials: Credentials,
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
                credentials_id=persisted_credentials.credentials_id,
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
        persisted_credentials: Credentials,
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
                credentials_id=persisted_credentials.credentials_id,
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
    async def test_component_strategy_execution(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
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
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()

        async def gen(entity_name, metrics, **driver_kwargs):
            yield {"id": 1, "name": "John"}

        mock_receiver.read_row = gen
        read_comp._receiver = mock_receiver

        strategy = Mock(spec=ExecutionStrategy)

        async def exec_gen(component, payload, metrics):
            async for item in read_comp.process_row(payload, metrics):
                yield item

        strategy.execute = exec_gen
        read_comp._strategy = strategy

        outs = []
        async for out in read_comp.execute({"id": 1}, mock_metrics):
            outs.append(out.payload)

        assert len(outs) == 1 and outs[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_error_propagation(
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
    async def test_metrics_integration(
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

    @pytest.mark.asyncio
    async def test_connection_handler_integration(
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
        assert hasattr(comp, "_connection_handler")
        assert hasattr(comp, "_receiver")

    @pytest.mark.asyncio
    async def test_bulk_strategy_integration(
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
                description="Test",
                comp_type="read_postgresql",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
                strategy_type="bulk",
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = sample_dataframe
        comp._receiver = mock_receiver

        strategy = Mock(spec=ExecutionStrategy)

        async def exec_gen(component, payload, metrics):
            gen = comp.process_bulk(payload, metrics)
            out = await anext(gen)
            yield out

        strategy.execute = exec_gen
        comp._strategy = strategy

        outs = []
        async for out in comp.execute(sample_dataframe, mock_metrics):
            outs.append(out.payload)

        assert len(outs) == 1 and len(outs[0]) == 2

    @pytest.mark.asyncio
    async def test_error_recovery_integration(
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
        calls = 0

        async def gen(entity_name, metrics, **driver_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise Exception("Temporary database error")
            yield {"id": 1, "name": "John"}

        mock_receiver.read_row = gen
        comp._receiver = mock_receiver

        with pytest.raises(Exception, match="Temporary database error"):
            async for _ in comp.process_row({"id": 1}, mock_metrics):
                pass

    @pytest.mark.asyncio
    async def test_large_dataset_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        large_df = pd.DataFrame(
            [
                {"id": i, "name": f"User{i}", "email": f"user{i}@example.com"}
                for i in range(1000)
            ]
        )

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
        mock_receiver.read_bulk.return_value = large_df
        comp._receiver = mock_receiver

        outs = []
        async for out in comp.process_bulk(large_df, mock_metrics):
            outs.append(out.payload)

        assert len(outs) == 1
        assert len(outs[0]) == 1000
        assert outs[0].iloc[0]["id"] == 0
        assert outs[0].iloc[999]["id"] == 999

    @pytest.mark.asyncio
    async def test_concurrent_operations_integration(
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

        async def run_once():
            res = []
            async for out in comp.process_row({"id": 1}, mock_metrics):
                res.append(out.payload)
            return res

        results = await asyncio.gather(*[run_once() for _ in range(5)])
        assert len(results) == 5
        for lst in results:
            assert len(lst) == 1 and lst[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_data_transformation_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        source = [
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
            for row in source:
                yield row

        mock_receiver.read_row = gen
        comp._receiver = mock_receiver

        transformed = []
        async for out in comp.process_row({"id": 1}, mock_metrics):
            p = out.payload
            transformed.append(
                {
                    "id": p["id"],
                    "full_name": f"{p['first_name']} {p['last_name']}",
                    "email": p["email"],
                }
            )

        assert len(transformed) == 2
        assert transformed[0]["full_name"] == "John Doe"
        assert transformed[1]["full_name"] == "Jane Smith"

    @pytest.mark.asyncio
    async def test_connection_pool_integration(
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
        assert hasattr(comp, "_connection_handler")

    @pytest.mark.asyncio
    async def test_metrics_performance_integration(
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
            await asyncio.sleep(0.005)
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

    @pytest.mark.asyncio
    async def test_error_handling_strategy_integration(
        self, persisted_credentials, mock_metrics
    ):
        """Test error handling strategy integration."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=persisted_credentials.credentials_id,
        )

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
