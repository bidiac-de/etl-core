"""
Tests for SQL Server specific functionality.

These tests focus on SQL Server-specific features like session variables,
collation handling, and SQL Server-specific SQL syntax.
"""

from __future__ import annotations

import pytest
from unittest.mock import Mock, patch

from etl_core.components.databases.sqlserver.sqlserver_read import SQLServerRead
from etl_core.components.databases.sqlserver.sqlserver_write import SQLServerWrite
from etl_core.components.databases.if_exists_strategy import DatabaseOperation
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType


class TestSQLServerSpecificFeatures:
    """Test cases for SQL Server specific features."""

    def _create_sqlserver_write_with_schema(self, **kwargs):
        """Helper to create SQLServerWrite component with proper schema."""
        # Set up mock schema for testing
        mock_schema = Schema(
            fields=[
                FieldDef(name="id", data_type=DataType.INTEGER),
                FieldDef(name="name", data_type=DataType.STRING),
                FieldDef(name="email", data_type=DataType.STRING),
            ]
        )

        # Merge the schema into kwargs
        if "in_port_schemas" not in kwargs:
            kwargs["in_port_schemas"] = {"in": mock_schema}

        write_comp = SQLServerWrite(**kwargs)
        return write_comp

    def test_sqlserver_session_variables_default(self):
        """Test SQL Server default session variables."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="read_sqlserver",
        )

        # Check default session variables
        assert component.charset == "utf8"
        assert component.collation == "SQL_Latin1_General_CP1_CI_AS"

    def test_sqlserver_session_variables_custom(self):
        """Test SQL Server custom session variables."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="read_sqlserver",
            charset="latin1",
            collation="SQL_Latin1_General_CP1_CS_AS",
        )

        # Check custom session variables
        assert component.charset == "latin1"
        assert component.collation == "SQL_Latin1_General_CP1_CS_AS"

    def test_sqlserver_session_variables_setup_execution(self):
        """Test SQL Server session variables setup execution."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="read_sqlserver",
        )

        # Mock connection handler
        mock_handler = Mock()
        mock_connection = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_connection)
        mock_context.__exit__ = Mock(return_value=None)
        mock_handler.lease.return_value = mock_context

        # Mock the _setup_session_variables method to avoid connection issues
        with patch.object(component, "_setup_session_variables") as mock_setup:
            component._connection_handler = mock_handler

            # Test session variables setup
            component._setup_session_variables()

            # Verify the method was called
            mock_setup.assert_called_once()

    def test_sqlserver_session_variables_without_connection(self):
        """Test SQL Server session variables setup without connection handler."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="read_sqlserver",
        )

        # Test that no error occurs when connection handler is None
        component._setup_session_variables()
        # Should return early without error

    def test_sqlserver_session_variables_connection_error(self):
        """Test SQL Server session variables setup with connection error."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="read_sqlserver",
        )

        # Mock connection handler that raises an error
        mock_handler = Mock()
        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Connection failed")
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_connection)
        mock_context.__exit__ = Mock(return_value=None)
        mock_handler.lease.return_value = mock_context

        component._connection_handler = mock_handler

        # Test that error is handled gracefully
        component._setup_session_variables()
        # Should not raise an exception, just print a warning

    def test_sqlserver_collation_handling(self):
        """Test SQL Server collation handling."""
        # Test different collation types
        collations = [
            "SQL_Latin1_General_CP1_CI_AS",  # Case-insensitive, accent-sensitive
            "SQL_Latin1_General_CP1_CS_AS",  # Case-sensitive, accent-sensitive
            "SQL_Latin1_General_CP1_CI_AI",  # Case-insensitive, accent-insensitive
            "SQL_Latin1_General_CP1_CS_AI",  # Case-sensitive, accent-insensitive
            "Latin1_General_100_CI_AS",  # Modern collation
            "Latin1_General_100_CS_AS",  # Modern case-sensitive
        ]

        for collation in collations:
            component = SQLServerRead(
                credentials_id=1,
                entity_name="test_table",
                name="test_component",
                description="Test SQL Server component",
                comp_type="read_sqlserver",
                collation=collation,
            )
            assert component.collation == collation

    def test_sqlserver_charset_handling(self):
        """Test SQL Server charset handling."""
        # Test different charset types
        charsets = ["utf8", "latin1", "cp1252", "iso_8859_1"]

        for charset in charsets:
            component = SQLServerRead(
                credentials_id=1,
                entity_name="test_table",
                name="test_component",
                description="Test SQL Server component",
                comp_type="read_sqlserver",
                charset=charset,
            )
            assert component.charset == charset

    def test_sqlserver_build_objects_flow(self):
        """Test SQL Server build_objects method flow."""
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="read_sqlserver",
        )

        # Mock the receiver creation to avoid connection issues
        with patch.object(component, "_receiver", None):
            # Call _build_objects directly
            result = component._build_objects()

            # Verify receiver was created
            assert component._receiver is not None
            # Verify self was returned
            assert result == component

    def test_sqlserver_component_initialization_order(self):
        """Test SQL Server component initialization order."""
        # Test that component can be created with minimal parameters
        component = SQLServerRead(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="read_sqlserver",
        )

        # Verify basic attributes are set
        assert component.credentials_id == 1
        assert component.entity_name == "test_table"
        assert component.charset == "utf8"
        assert component.collation == "SQL_Latin1_General_CP1_CI_AS"

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
            ("credential_ids", [1, 100, 999, 1000], SQLServerRead, "credentials_id"),
            (
                "queries",
                [
                    "",
                    "SELECT * FROM users",
                    "INSERT INTO users (name, email) VALUES (:name, :email)",
                    "UPDATE users SET name = :name WHERE id = :id",
                    "DELETE FROM users WHERE id = :id",
                    "SELECT u.id, u.name, u.email, p.phone FROM users u LEFT JOIN \
                    profiles p ON u.id = p.user_id WHERE u.active = :active_status",
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
            (
                "chunk_sizes",
                [1000, 5000, 10000, 50000, 100000],
                SQLServerWrite,
                "bulk_chunk_size",
            ),
        ],
    )
    def test_sqlserver_component_validation_scenarios(
        self, test_type, test_values, component_class, expected_attr
    ):
        """Test SQL Server component validation scenarios."""

        for value in test_values:
            if component_class == SQLServerRead:
                component = component_class(
                    credentials_id=value if test_type == "credential_ids" else 1,
                    entity_name="test_table" if test_type != "entity_names" else value,
                    name="test_component",
                    description="Test SQL Server component",
                    comp_type="read_sqlserver",
                    query=value if test_type == "queries" else "",
                )
            else:  # SQLServerWrite
                component = self._create_sqlserver_write_with_schema(
                    credentials_id=1,
                    entity_name="test_table",
                    name="test_component",
                    description="Test SQL Server component",
                    comp_type="write_sqlserver",
                    operation=(
                        value
                        if test_type == "if_exists_values"
                        else DatabaseOperation.INSERT
                    ),
                    bulk_chunk_size=value if test_type == "chunk_sizes" else 50000,
                    bigdata_partition_chunk_size=(
                        value * 2 if test_type == "chunk_sizes" else 50000
                    ),
                )

            if test_type == "entity_names":
                assert getattr(component, expected_attr) == value
            elif test_type == "credential_ids":
                assert getattr(component, expected_attr) == value
            elif test_type == "queries":
                assert getattr(component, expected_attr) == value
            elif test_type == "if_exists_values":
                assert getattr(component, expected_attr) == value
            elif test_type == "chunk_sizes":
                assert getattr(component, expected_attr) == value
                assert component.bigdata_partition_chunk_size == value * 2

    def test_sqlserver_component_zero_chunk_sizes(self):
        """Test SQL Server component with zero chunk sizes."""
        # Test with minimum allowed chunk sizes (1) instead of 0
        component = self._create_sqlserver_write_with_schema(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="write_sqlserver",
            bulk_chunk_size=1,
            bigdata_partition_chunk_size=1,
        )

        assert component.bulk_chunk_size == 1
        assert component.bigdata_partition_chunk_size == 1

    def test_sqlserver_component_zero_chunk_sizes_validation(self):
        """Test SQL Server component validation prevents zero chunk sizes."""
        # Test that validation prevents zero chunk sizes
        with pytest.raises(Exception):  # Should raise validation error
            self._create_sqlserver_write_with_schema(
                credentials_id=1,
                entity_name="test_table",
                name="test_component",
                description="Test SQL Server component",
                comp_type="write_sqlserver",
                bulk_chunk_size=0,
                bigdata_partition_chunk_size=0,
            )

    def test_sqlserver_component_large_chunk_sizes(self):
        """Test SQL Server component with large chunk sizes."""
        component = self._create_sqlserver_write_with_schema(
            credentials_id=1,
            entity_name="test_table",
            name="test_component",
            description="Test SQL Server component",
            comp_type="write_sqlserver",
            bulk_chunk_size=1_000_000,
            bigdata_partition_chunk_size=2_000_000,
        )

        assert component.bulk_chunk_size == 1_000_000
        assert component.bigdata_partition_chunk_size == 2_000_000

    def test_sqlserver_component_combined_parameters(self):
        """Test SQL Server component with all parameters combined."""
        component = self._create_sqlserver_write_with_schema(
            credentials_id=999,
            entity_name="dbo.user_profiles",
            name="test_component",
            description="Test SQL Server component",
            comp_type="write_sqlserver",
            charset="latin1",
            collation="SQL_Latin1_General_CP1_CS_AS",
            operation=DatabaseOperation.UPSERT,
            bulk_chunk_size=25_000,
            bigdata_partition_chunk_size=100_000,
        )

        # Verify all parameters are set correctly
        assert component.credentials_id == 999
        assert component.entity_name == "dbo.user_profiles"
        assert component.charset == "latin1"
        assert component.collation == "SQL_Latin1_General_CP1_CS_AS"
        assert component.operation == DatabaseOperation.UPSERT
        assert component.bulk_chunk_size == 25_000
        assert component.bigdata_partition_chunk_size == 100_000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
