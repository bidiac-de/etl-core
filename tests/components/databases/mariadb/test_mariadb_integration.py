"""
Integration tests for MariaDB components (mocked receivers/handler).

Updated: assert active credentials via _get_credentials()["__credentials_id__"].
"""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock, patch

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.components.databases.mariadb.mariadb_read import MariaDBRead
from etl_core.components.databases.mariadb.mariadb_write import MariaDBWrite
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics


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
        persisted_mapping_context_id: str,
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
                context_id=persisted_mapping_context_id,
            )
            write_comp = self._create_mariadb_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_mariadb",
                entity_name="users",
                context_id=persisted_mapping_context_id,
            )

        # Sanity: both components resolved credentials ID
        assert read_comp._get_credentials()["__credentials_id__"]
        assert write_comp._get_credentials()["__credentials_id__"]

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
        persisted_mapping_context_id: str,
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
                context_id=persisted_mapping_context_id,
            )

        mock_read_receiver = AsyncMock()

        async def mock_read_row_generator(entity_name, metrics, **driver_kwargs):
            for item in sample_data:
                yield item

        mock_read_receiver.read_row = mock_read_row_generator
        read_comp._receiver = mock_read_receiver

        class _DummyStrategy:
            async def execute(self, component, payload, metrics):
                async for item in component.process_row(payload, metrics):
                    yield item

        read_comp._strategy = _DummyStrategy()  # type: ignore[attr-defined]

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
        persisted_mapping_context_id: str,
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
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_bigdata.return_value = sample_ddf
        read_comp._receiver = mock_receiver

        class _DummyStrategy:
            async def execute(self, component, payload, metrics):
                gen = component.process_bigdata(payload, metrics)
                out = await anext(gen)
                yield out

        read_comp._strategy = _DummyStrategy()  # type: ignore[attr-defined]

        results = []
        async for result in read_comp.execute(sample_ddf, mock_metrics):
            results.append(result.payload)

        assert len(results) == 1
        assert hasattr(results[0], "npartitions")

    @pytest.mark.asyncio
    async def test_component_strategy_execution(
        self, persisted_mapping_context_id: str, mock_metrics: ComponentMetrics
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

    @pytest.mark.asyncio
    async def test_error_propagation(
        self, persisted_mapping_context_id: str, mock_metrics: ComponentMetrics
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
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_row.side_effect = Exception("Database error")
        read_comp._receiver = mock_receiver

        with pytest.raises(Exception):
            async for _ in read_comp.process_row({"id": 1}, mock_metrics):
                pass

    @pytest.mark.asyncio
    async def test_metrics_integration(
        self, persisted_mapping_context_id: str, mock_metrics: ComponentMetrics
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
                context_id=persisted_mapping_context_id,
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
        self, persisted_mapping_context_id: str
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
                context_id=persisted_mapping_context_id,
            )

        assert read_comp.name == "test_read"
        assert read_comp.comp_type == "read_mariadb"
        assert read_comp.entity_name == "users"
        assert read_comp.query == "SELECT * FROM users"
