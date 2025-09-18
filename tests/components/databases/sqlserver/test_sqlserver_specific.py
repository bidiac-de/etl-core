"""
SQL Server–specific behavior (charset, collation, session vars), updated to context_id.
"""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from etl_core.components.databases.if_exists_strategy import DatabaseOperation
from etl_core.components.databases.sqlserver.sqlserver_read import SQLServerRead
from etl_core.components.databases.sqlserver.sqlserver_write import SQLServerWrite
from etl_core.components.wiring.column_definition import DataType, FieldDef
from etl_core.components.wiring.schema import Schema


class TestSQLServerSpecificFeatures:
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

    def test_sqlserver_session_variables_default(
        self, persisted_mapping_context_id: str
    ):
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = SQLServerRead(
                name="test_component",
                description="Test SQL Server component",
                comp_type="read_sqlserver",
                entity_name="test_table",
                context_id=persisted_mapping_context_id,
            )
        assert comp.charset == "utf8"
        assert comp.collation == "SQL_Latin1_General_CP1_CI_AS"

    def test_sqlserver_session_variables_custom(
        self, persisted_mapping_context_id: str
    ):
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = SQLServerRead(
                name="test_component",
                description="Test SQL Server component",
                comp_type="read_sqlserver",
                entity_name="test_table",
                charset="latin1",
                collation="SQL_Latin1_General_CP1_CS_AS",
                context_id=persisted_mapping_context_id,
            )
        assert comp.charset == "latin1"
        assert comp.collation == "SQL_Latin1_General_CP1_CS_AS"

    def test_sqlserver_session_variables_setup_execution(
        self, persisted_mapping_context_id: str
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = SQLServerRead(
                name="test_component",
                description="Test SQL Server component",
                comp_type="read_sqlserver",
                entity_name="test_table",
                context_id=persisted_mapping_context_id,
            )

        mock_handler = Mock()
        mock_connection = Mock()
        mock_ctx = Mock()
        mock_ctx.__enter__ = Mock(return_value=mock_connection)
        mock_ctx.__exit__ = Mock(return_value=None)
        mock_handler.lease.return_value = mock_ctx
        comp._connection_handler = mock_handler

        with patch.object(comp, "_setup_session_variables") as mock_setup:
            comp._setup_session_variables()
            mock_setup.assert_called_once()

    def test_sqlserver_session_variables_without_connection(
        self, persisted_mapping_context_id: str
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = SQLServerRead(
                name="test_component",
                description="Test SQL Server component",
                comp_type="read_sqlserver",
                entity_name="test_table",
                context_id=persisted_mapping_context_id,
            )
        # Should be a no-op without raising
        comp._setup_session_variables()

    def test_sqlserver_session_variables_connection_error(
        self, persisted_mapping_context_id: str
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = SQLServerRead(
                name="test_component",
                description="Test SQL Server component",
                comp_type="read_sqlserver",
                entity_name="test_table",
                context_id=persisted_mapping_context_id,
            )

        mock_handler = Mock()
        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Connection failed")
        mock_ctx = Mock()
        mock_ctx.__enter__ = Mock(return_value=mock_connection)
        mock_ctx.__exit__ = Mock(return_value=None)
        mock_handler.lease.return_value = mock_ctx
        comp._connection_handler = mock_handler

        # Should not raise
        comp._setup_session_variables()

    @pytest.mark.parametrize(
        "collation",
        [
            "SQL_Latin1_General_CP1_CI_AS",
            "SQL_Latin1_General_CP1_CS_AS",
            "SQL_Latin1_General_CP1_CI_AI",
            "SQL_Latin1_General_CP1_CS_AI",
            "Latin1_General_100_CI_AS",
            "Latin1_General_100_CS_AS",
        ],
    )
    def test_sqlserver_collation_handling(
        self, collation: str, persisted_mapping_context_id: str
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = SQLServerRead(
                name="test_component",
                description="Test SQL Server component",
                comp_type="read_sqlserver",
                entity_name="test_table",
                collation=collation,
                context_id=persisted_mapping_context_id,
            )
        assert comp.collation == collation

    @pytest.mark.parametrize("charset", ["utf8", "latin1", "cp1252", "iso_8859_1"])
    def test_sqlserver_charset_handling(
        self, charset: str, persisted_mapping_context_id: str
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = SQLServerRead(
                name="test_component",
                description="Test SQL Server component",
                comp_type="read_sqlserver",
                entity_name="test_table",
                charset=charset,
                context_id=persisted_mapping_context_id,
            )
        assert comp.charset == charset

    def test_sqlserver_build_objects_flow(
        self, persisted_mapping_context_id: str
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = SQLServerRead(
                name="test_component",
                description="Test SQL Server component",
                comp_type="read_sqlserver",
                entity_name="test_table",
                context_id=persisted_mapping_context_id,
            )
        # Receiver gets created during build
        result = comp._build_objects()
        assert comp._receiver is not None
        assert result == comp

    @pytest.mark.parametrize(
        "test_type,test_values,component_class,expected_attr",
        [
            (
                "entity_names",
                [
                    "users",
                    "dbo.users",
                    "[Users]",
                    "MyTable",
                    "table_123",
                    "user_profiles",
                ],
                SQLServerRead,
                "entity_name",
            ),
            (
                "queries",
                [
                    "",
                    "SELECT * FROM users",
                    "UPDATE users SET name = :name WHERE id = :id",
                ],
                SQLServerRead,
                "query",
            ),
            (
                "if_exists_values",
                [
                    DatabaseOperation.INSERT,
                    DatabaseOperation.UPSERT,
                    DatabaseOperation.TRUNCATE,
                ],
                SQLServerWrite,
                "operation",
            ),
            ("chunk_sizes", [1000, 5000, 10000], SQLServerWrite, "bulk_chunk_size"),
        ],
    )
    def test_sqlserver_component_validation_scenarios(
        self,
        test_type,
        test_values,
        component_class,
        expected_attr,
        persisted_mapping_context_id: str,
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            for value in test_values:
                if component_class == SQLServerRead:
                    comp = component_class(
                        name="test_component",
                        description="Test SQL Server component",
                        comp_type="read_sqlserver",
                        entity_name=(
                            "test_table" if test_type != "entity_names" else value
                        ),
                        query=value if test_type == "queries" else "",
                        context_id=persisted_mapping_context_id,
                    )
                else:
                    comp = self._create_sqlserver_write_with_schema(
                        name="test_component",
                        description="Test SQL Server component",
                        comp_type="write_sqlserver",
                        entity_name="test_table",
                        operation=(
                            value
                            if test_type == "if_exists_values"
                            else DatabaseOperation.INSERT
                        ),
                        bulk_chunk_size=value if test_type == "chunk_sizes" else 50000,
                        bigdata_partition_chunk_size=(
                            value * 2 if test_type == "chunk_sizes" else 50000
                        ),
                        context_id=persisted_mapping_context_id,
                    )
                if test_type == "queries":
                    assert value == getattr(comp, expected_attr)
                else:
                    assert getattr(comp, expected_attr) == value

    def test_sqlserver_component_zero_chunk_sizes(
        self, persisted_mapping_context_id: str
    ) -> None:
        with patch(
            "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
        ):
            comp = self._create_sqlserver_write_with_schema(
                name="test_component",
                description="Test SQL Server component",
                comp_type="write_sqlserver",
                entity_name="test_table",
                bulk_chunk_size=1,
                bigdata_partition_chunk_size=2,
                context_id=persisted_mapping_context_id,
            )
            assert comp.bulk_chunk_size == 1
            assert comp.bigdata_partition_chunk_size == 2
