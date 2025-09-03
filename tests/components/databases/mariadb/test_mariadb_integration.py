"""
Integration tests for MariaDB components (mocked receivers/handler).

Updated: credentials are resolved directly via credentials_id; no Context required.
"""

from __future__ import annotations

import asyncio
import os
from typing import Tuple
from uuid import uuid4
from unittest.mock import Mock, AsyncMock, patch

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.components.databases.mariadb.mariadb_read import MariaDBRead
from etl_core.components.databases.mariadb.mariadb_write import MariaDBWrite
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.strategies.base_strategy import ExecutionStrategy
from etl_core.context.credentials import Credentials
from etl_core.persistance.handlers.credentials_handler import CredentialsHandler


class TestMariaDBIntegration:
    """Test MariaDB integration scenarios."""

    def _create_mariadb_write_with_schema(self, **kwargs) -> MariaDBWrite:
        """Helper to create MariaDBWrite component with proper schema."""
        from etl_core.components.wiring.schema import Schema
        from etl_core.components.wiring.column_definition import FieldDef, DataType

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
    def test_creds(self) -> Tuple[str, str]:
        return os.environ["APP_TEST_USER"], os.environ["APP_TEST_PASSWORD"]

    @pytest.fixture
    def persisted_credentials(self, test_creds: Tuple[str, str]) -> Credentials:
        """Persist real Credentials so components can resolve them by credentials_id."""
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
    def sample_ddf(self) -> dd.DataFrame:
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
        self,
        persisted_credentials: Credentials,
        mock_metrics: ComponentMetrics,
        sample_data,
    ) -> None:
        """Test complete read to write pipeline."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )
            write_comp = self._create_mariadb_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_read_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            for item in sample_data:
                yield item

        mock_read_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_read_receiver

        mock_write_receiver = AsyncMock()
        mock_write_receiver.write_row.return_value = {
            "affected_rows": 1,
            "row": {"id": 1, "name": "John"},
        }
        write_comp._receiver = mock_write_receiver

        read_results = []
        async for result in read_comp.process_row({"id": 1}, mock_metrics):
            read_results.append(result.payload)
        assert len(read_results) == 2

        write_results = []
        async for result in write_comp.process_row(read_results[0], mock_metrics):
            write_results.append(result.payload)

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
        """Test row strategy streaming with MariaDB components."""
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
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_read_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            for item in sample_data:
                yield item

        mock_read_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_read_receiver

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

        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[1]["id"] == 2

    @pytest.mark.asyncio
    async def test_bigdata_strategy(
        self,
        persisted_credentials: Credentials,
        mock_metrics: ComponentMetrics,
        sample_ddf: dd.DataFrame,
    ) -> None:
        """Test bigdata strategy with MariaDB components."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_bigdata.return_value = sample_ddf
        read_comp._receiver = mock_receiver

        mock_strategy = Mock(spec=ExecutionStrategy)

        async def mock_execute_generator(component, payload, metrics):
            gen = read_comp.process_bigdata(payload, metrics)
            out = await anext(gen)
            yield out

        mock_strategy.execute = mock_execute_generator
        read_comp._strategy = mock_strategy

        results = []
        async for result in read_comp.execute(sample_ddf, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1
        assert hasattr(results[0], "npartitions")

    @pytest.mark.asyncio
    async def test_component_strategy_execution(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        """Test component strategy execution."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
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

    @pytest.mark.asyncio
    async def test_error_propagation(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        """Test error propagation through the pipeline."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_row.side_effect = Exception("Database error")
        read_comp._receiver = mock_receiver

        with pytest.raises(Exception):
            async for _ in read_comp.process_row({"id": 1}, mock_metrics):
                pass

    @pytest.mark.asyncio
    async def test_metrics_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        """Test metrics integration with MariaDB components."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            mock_metrics.set_started()
            yield {"id": 1, "name": "John"}
            mock_metrics.set_completed()

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        payload = {"id": 1}
        results = []
        async for result in read_comp.process_row(payload, mock_metrics):
            results.append(result.payload)

        mock_metrics.set_started.assert_called_once()
        mock_metrics.set_completed.assert_called_once()
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_connection_handler_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        """Test connection handler integration (construction only)."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        assert read_comp.name == "test_read"
        assert read_comp.comp_type == "read_mariadb"
        assert read_comp.entity_name == "users"
        assert read_comp.query == "SELECT * FROM users"
        assert read_comp.credentials_id == persisted_credentials.credentials_id
        assert hasattr(read_comp, "_connection_handler")
        assert hasattr(read_comp, "_receiver")

    @pytest.mark.asyncio
    async def test_bulk_strategy_integration(
        self,
        persisted_credentials: Credentials,
        mock_metrics: ComponentMetrics,
        sample_dataframe: pd.DataFrame,
    ) -> None:
        """Test bulk strategy integration with MariaDB components."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
                strategy_type="bulk",
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = sample_dataframe
        read_comp._receiver = mock_receiver

        mock_strategy = Mock(spec=ExecutionStrategy)

        async def mock_execute_generator(component, payload, metrics):
            gen = read_comp.process_bulk(payload, metrics)
            out = await anext(gen)
            yield out

        mock_strategy.execute = mock_execute_generator
        read_comp._strategy = mock_strategy

        payload = sample_dataframe
        results = []
        async for result in read_comp.execute(payload, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1
        assert len(results[0]) == 2

    @pytest.mark.asyncio
    async def test_error_recovery_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        """Test error recovery and retry logic in integration."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()
        call_count = 0

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Temporary database error")
            yield {"id": 1, "name": "John"}

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        with pytest.raises(Exception, match="Temporary database error"):
            async for _ in read_comp.process_row({"id": 1}, mock_metrics):
                pass

    @pytest.mark.asyncio
    async def test_large_dataset_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        """Test integration with large datasets."""
        large_data = [
            {"id": i, "name": f"User{i}", "email": f"user{i}@example.com"}
            for i in range(1000)
        ]
        large_df = pd.DataFrame(large_data)

        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = large_df
        read_comp._receiver = mock_receiver

        gen = read_comp.process_bulk(large_df, mock_metrics)
        result = await anext(gen)

        assert len(result.payload) == 1000
        assert result.payload.iloc[0]["id"] == 0
        assert result.payload.iloc[999]["id"] == 999

    @pytest.mark.asyncio
    async def test_concurrent_operations_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        """Test concurrent operations in integration."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            yield {"id": 1, "name": "John"}

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        async def concurrent_operation():
            results = []
            async for result in read_comp.process_row({"id": 1}, mock_metrics):
                results.append(result.payload)
            return results

        tasks = [concurrent_operation() for _ in range(5)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 5
        for result_list in results:
            assert len(result_list) == 1
            assert result_list[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_data_transformation_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        """Test data transformation through the pipeline."""
        source_data = [
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
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_read_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            for item in source_data:
                yield item

        mock_read_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_read_receiver

        transformed_data = []
        async for result in read_comp.process_row({"id": 1}, mock_metrics):
            res = result.payload
            transformed = {
                "id": res["id"],
                "full_name": f"{res['first_name']} {res['last_name']}",
                "email": res["email"],
            }
            transformed_data.append(transformed)

        assert len(transformed_data) == 2
        assert transformed_data[0]["full_name"] == "John Doe"
        assert transformed_data[1]["full_name"] == "Jane Smith"

    @pytest.mark.asyncio
    async def test_connection_pool_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        """Test connection pool integration (construction only)."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        assert read_comp.name == "test_read"
        assert read_comp.comp_type == "read_mariadb"
        assert read_comp.entity_name == "users"
        assert read_comp.query == "SELECT * FROM users"
        assert read_comp.credentials_id == persisted_credentials.credentials_id
        assert hasattr(read_comp, "_connection_handler")
        assert hasattr(read_comp, "_receiver")

    @pytest.mark.asyncio
    async def test_metrics_performance_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        """Test metrics performance tracking in integration."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            mock_metrics.set_started()
            await asyncio.sleep(0.01)
            yield {"id": 1, "name": "John"}
            mock_metrics.set_completed()

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        results = []
        async for result in read_comp.process_row({"id": 1}, mock_metrics):
            results.append(result.payload)
        end_time = asyncio.get_event_loop().time()

        mock_metrics.set_started.assert_called_once()
        mock_metrics.set_completed.assert_called_once()
        assert len(results) == 1
        assert end_time >= 0

    @pytest.mark.asyncio
    async def test_error_handling_strategy_integration(
        self, persisted_credentials: Credentials, mock_metrics: ComponentMetrics
    ) -> None:
        """Test error handling strategy integration."""
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            read_comp = MariaDBRead(
                name="test_read",
                description="Test read component",
                comp_type="read_mariadb",
                entity_name="users",
                query="SELECT * FROM users",
                credentials_id=persisted_credentials.credentials_id,
            )

        mock_receiver = AsyncMock()
        error_scenarios = [
            Exception("Connection timeout"),
            Exception("Query syntax error"),
            Exception("Permission denied"),
            {"id": 1, "name": "John"},
        ]

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            for scenario in error_scenarios:
                if isinstance(scenario, Exception):
                    raise scenario
                yield scenario

        mock_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_receiver

        with pytest.raises(Exception, match="Connection timeout"):
            async for _ in read_comp.process_row({"id": 1}, mock_metrics):
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
