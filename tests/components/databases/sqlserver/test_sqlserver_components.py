"""
Unit tests for SQL Server ETL components (mocked receivers/handler).

All components accept a mapping-context via `context_id` and resolve real creds.
"""

from __future__ import annotations

from typing import Tuple
from unittest.mock import AsyncMock, Mock, patch

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.components.databases.sqlserver.sqlserver_read import SQLServerRead
from etl_core.components.databases.sqlserver.sqlserver_write import SQLServerWrite
from etl_core.components.databases.if_exists_strategy import DatabaseOperation
from etl_core.components.wiring.column_definition import DataType, FieldDef
from etl_core.components.wiring.schema import Schema
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.context.credentials import Credentials


class TestSQLServerComponents:
    """Test cases for SQL Server components."""

    def _mk_schema(self) -> Schema:
        return Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )

    def _create_sqlserver_write_with_schema(self, **kwargs) -> SQLServerWrite:
        if "in_port_schemas" not in kwargs:
            kwargs["in_port_schemas"] = {"in": self._mk_schema()}
        return SQLServerWrite(**kwargs)

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
            {"id": [1, 2], "name": ["John", "Jane"], "email": ["john@example.com", "jane@example.com"]}
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
        return dd.from_pandas(df, npartitions=2)

    def test_sqlserver_read_initialization(
        self,
        persisted_mapping_context_id: str,
        persisted_credentials: Credentials,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = SQLServerRead(
                name="test_read",
                description="Test read component",
                comp_type="read_sqlserver",
                entity_name="users",
                query="SELECT * FROM users",
                params={"limit": 10},
                context_id=persisted_mapping_context_id,
            )

        assert comp.query == "SELECT * FROM users"
        assert comp.params == {"limit": 10}
        active_id = comp._get_credentials()["__credentials_id__"]
        assert active_id == persisted_credentials.credentials_id
        assert comp._credentials is not None

    def test_sqlserver_write_initialization(
        self,
        persisted_mapping_context_id: str,
        persisted_credentials: Credentials,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = self._create_sqlserver_write_with_schema(
                name="test_write",
                description="Test write component",
                comp_type="write_sqlserver",
                entity_name="users",
                context_id=persisted_mapping_context_id,
            )

        assert comp.entity_name == "users"
        active_id = comp._get_credentials()["__credentials_id__"]
        assert active_id == persisted_credentials.credentials_id

    @pytest.mark.asyncio
    async def test_sqlserver_read_process_row(
        self,
        persisted_mapping_context_id: str,
        mock_metrics: ComponentMetrics,
        sample_data,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = SQLServerRead(
                name="test_read",
                description="Test read",
                comp_type="read_sqlserver",
                entity_name="users",
                query="SELECT * FROM users WHERE id = %(id)s",
                params={"id": 1},
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()

        async def gen(*, entity_name, metrics, connection_handler, batch_size=1000, query=None, params=None):
            for item in sample_data:
                yield item

        mock_receiver.read_row = gen
        comp._receiver = mock_receiver

        results = []
        async for out in comp.process_row({"id": 1}, mock_metrics):
            results.append(out.payload)

        assert [r["id"] for r in results] == [1, 2]

    @pytest.mark.asyncio
    async def test_sqlserver_read_process_bulk(
        self,
        persisted_mapping_context_id: str,
        mock_metrics: ComponentMetrics,
        sample_dataframe: pd.DataFrame,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = SQLServerRead(
                name="test_read",
                description="Test read",
                comp_type="read_sqlserver",
                entity_name="users",
                query="SELECT * FROM users",
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_bulk.return_value = sample_dataframe
        comp._receiver = mock_receiver

        outs = []
        async for out in comp.process_bulk(sample_dataframe, mock_metrics):
            outs.append(out.payload)

        assert len(outs) == 1 and len(outs[0]) == 2

    @pytest.mark.asyncio
    async def test_sqlserver_read_process_bigdata(
        self,
        persisted_mapping_context_id: str,
        mock_metrics: ComponentMetrics,
        sample_dask_dataframe: dd.DataFrame,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = SQLServerRead(
                name="test_read",
                description="Test read",
                comp_type="read_sqlserver",
                entity_name="users",
                query="SELECT * FROM users",
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.read_bigdata.return_value = sample_dask_dataframe
        comp._receiver = mock_receiver

        outs = []
        async for out in comp.process_bigdata(sample_dask_dataframe, mock_metrics):
            outs.append(out.payload)

        assert len(outs) == 1 and hasattr(outs[0], "npartitions")

    @pytest.mark.asyncio
    async def test_sqlserver_write_process_row(
        self, persisted_mapping_context_id: str, mock_metrics: ComponentMetrics
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = self._create_sqlserver_write_with_schema(
                name="test_write",
                description="Test write",
                comp_type="write_sqlserver",
                entity_name="users",
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()

        async def write_row(*, entity_name, row, metrics, connection_handler, query, table=None):
            return {"affected_rows": 1, "row": row}

        mock_receiver.write_row = write_row
        comp._receiver = mock_receiver

        outs = []
        async for out in comp.process_row({"name": "John"}, mock_metrics):
            outs.append(out.payload)

        assert len(outs) == 1 and outs[0]["affected_rows"] == 1

    @pytest.mark.asyncio
    async def test_sqlserver_write_process_bulk(
        self,
        persisted_mapping_context_id: str,
        mock_metrics: ComponentMetrics,
        sample_dataframe: pd.DataFrame,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = self._create_sqlserver_write_with_schema(
                name="test_write",
                description="Test write",
                comp_type="write_sqlserver",
                entity_name="users",
                context_id=persisted_mapping_context_id,
            )

        mock_receiver = AsyncMock()
        mock_receiver.write_bulk.return_value = sample_dataframe
        comp._receiver = mock_receiver

        outs = []
        async for out in comp.process_bulk(sample_dataframe, mock_metrics):
            outs.append(out.payload)

        assert len(outs) == 1 and len(outs[0]) == 2

    @pytest.mark.asyncio
    async def test_sqlserver_write_process_bigdata(
        self,
        persisted_mapping_context_id: str,
        mock_metrics: ComponentMetrics,
        sample_dask_dataframe: dd.DataFrame,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = self._create_sqlserver_write_with_schema(
                name="test_write",
                description="Test write",
                comp_type="write_sqlserver",
                entity_name="users",
                if_exists="replace",
                bigdata_partition_chunk_size=25_000,
                context_id=persisted_mapping_context_id,
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

    def test_sqlserver_component_defaults_and_attrs(
        self, persisted_mapping_context_id: str
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = SQLServerRead(
                name="test_read",
                description="Test",
                comp_type="read_sqlserver",
                entity_name="users",
                query="",
                context_id=persisted_mapping_context_id,
            )
        assert comp.entity_name == "users"
        assert hasattr(comp, "charset")
        assert hasattr(comp, "collation")
