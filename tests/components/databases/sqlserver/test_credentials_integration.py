"""
Integration tests for SQL Server credentials and context functionality.

These tests verify that the Credentials and Context classes work together
properly in real scenarios, using environment variables for sensitive data.
"""

import hashlib
from typing import Tuple

import pytest
from unittest.mock import Mock, patch

from etl_core.context.context import Context
from etl_core.context.credentials import Credentials
from etl_core.components.databases.sqlserver.sqlserver_read import SQLServerRead
from etl_core.components.databases.sqlserver.sqlserver_write import SQLServerWrite
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType


def derive_test_password(base_pw: str, purpose: str) -> str:
    """
    Deterministically derive a test password variant without hard-coded secrets.
    """
    digest = hashlib.blake2b(
        f"{purpose}:{base_pw}".encode("utf-8"), digest_size=6
    ).hexdigest()
    return f"{base_pw}_{digest}"


class TestSQLServerCredentialsIntegration:
    """Test cases for real SQL Server Credentials and Context integration."""

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

    def test_credentials_creation(
        self, sample_credentials: Credentials, test_creds: Tuple[str, str]
    ) -> None:
        user, password = test_creds
        assert sample_credentials.credentials_id == 1
        assert sample_credentials.name == "test_db_creds"
        assert sample_credentials.user == user
        assert sample_credentials.decrypted_password == password
        assert sample_credentials.pool_max_size == 10
        assert sample_credentials.pool_timeout_s == 30

    def test_credentials_get_parameter(
        self, sample_credentials: Credentials, test_creds: Tuple[str, str]
    ) -> None:
        user, _ = test_creds
        assert sample_credentials.get_parameter("user") == user
        assert sample_credentials.get_parameter("database") == "testdb"
        assert sample_credentials.get_parameter("pool_max_size") == 10
        assert sample_credentials.get_parameter("pool_timeout_s") == 30
        with pytest.raises(KeyError, match="Unknown parameter key: invalid_key"):
            sample_credentials.get_parameter("invalid_key")

    def test_context_credentials_management(
        self, sample_context: Context, sample_credentials: Credentials
    ) -> None:
        retrieved_creds = sample_context.get_credentials(1)
        assert retrieved_creds == sample_credentials
        with pytest.raises(KeyError, match="Credentials with ID 2 not found"):
            sample_context.get_credentials(2)

    def test_context_add_credentials(self, sample_context: Context, test_creds) -> None:
        _, password = test_creds
        new_creds = Credentials(
            credentials_id=2,
            name="new_creds",
            user="newuser",
            host="localhost",
            port=1433,  # SQL Server default port
            database="newdb",
            password=password,
        )
        sample_context.add_credentials(new_creds)
        retrieved = sample_context.get_credentials(2)
        assert retrieved.name == "new_creds"
        assert retrieved.user == "newuser"

    @patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
    def test_sqlserver_read_component_with_real_credentials(
        self, mock_handler_class, sample_context: Context, test_creds
    ) -> None:
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler
        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = sample_context
        creds = read_comp._get_credentials()
        assert creds["user"] == test_creds[0]
        assert creds["database"] == "testdb"
        assert creds["host"] == "localhost"
        assert creds["port"] == 1433

    @patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
    def test_sqlserver_write_component_with_real_credentials(
        self, mock_handler_class, sample_context: Context, test_creds
    ) -> None:
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler
        write_comp = self._create_sqlserver_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver",
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = sample_context
        creds = write_comp._get_credentials()
        assert creds["user"] == test_creds[0]
        assert creds["database"] == "testdb"
        assert creds["host"] == "localhost"
        assert creds["port"] == 1433

    def test_credentials_in_sqlserver_component_integration(
        self, sample_context: Context, test_creds
    ) -> None:
        """Test that credentials are properly integrated in SQL Server components."""
        user, password = test_creds

        # Test read component
        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = sample_context

        # Test write component
        write_comp = self._create_sqlserver_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver",
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = sample_context

        # Verify both components have access to credentials
        read_creds = read_comp._get_credentials()
        write_creds = write_comp._get_credentials()

        assert read_creds["user"] == user
        assert write_creds["user"] == user
        assert read_creds["database"] == "testdb"
        assert write_creds["database"] == "testdb"

    def test_sqlserver_write_bulk_operations(
        self, sample_context: Context, test_creds
    ) -> None:
        """Test SQL Server write component bulk operations with credentials."""
        # Create write component
        write_comp = self._create_sqlserver_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver",
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = sample_context

        # Verify credentials are accessible
        creds = write_comp._get_credentials()
        assert creds["user"] == test_creds[0]
        assert creds["database"] == "testdb"

    def test_sqlserver_component_fixtures(
        self, sample_context: Context, sample_credentials: Credentials, test_creds
    ) -> None:
        """Test that all fixtures work correctly with SQL Server components."""
        user, password = test_creds

        # Test context fixture
        assert sample_context is not None
        assert hasattr(sample_context, "get_credentials")

        # Test credentials fixture
        assert sample_credentials is not None
        assert sample_credentials.credentials_id == 1
        assert sample_credentials.user == user

        # Test test_creds fixture
        assert test_creds[0] == user
        assert test_creds[1] == password

    def test_credentials_encryption_decryption(
        self, sample_credentials: Credentials, test_creds
    ) -> None:
        """Test that credentials are properly encrypted and decrypted."""
        user, password = test_creds

        # Verify the credentials are properly encrypted/decrypted
        assert sample_credentials.user == user
        assert sample_credentials.decrypted_password == password

        # Verify the encrypted password is different from the plain text
        assert sample_credentials.password != password

    def test_credentials_parameter_access(
        self, sample_credentials: Credentials, test_creds
    ) -> None:
        """Test that all credential parameters are accessible."""
        user, password = test_creds

        # Test all available parameters
        assert sample_credentials.get_parameter("user") == user
        assert sample_credentials.get_parameter("host") == "localhost"
        assert sample_credentials.get_parameter("port") == 1433
        assert sample_credentials.get_parameter("database") == "testdb"
        assert sample_credentials.get_parameter("pool_max_size") == 10
        assert sample_credentials.get_parameter("pool_timeout_s") == 30

    def test_context_credentials_retrieval(
        self, sample_context: Context, sample_credentials: Credentials
    ) -> None:
        """Test that credentials can be retrieved from context by ID."""
        # Test retrieval by ID
        retrieved_creds = sample_context.get_credentials(1)
        assert retrieved_creds == sample_credentials

        # Test that the retrieved credentials have the same properties
        assert retrieved_creds.credentials_id == sample_credentials.credentials_id
        assert retrieved_creds.name == sample_credentials.name
        assert retrieved_creds.user == sample_credentials.user

    def test_credentials_validation(
        self, sample_credentials: Credentials, test_creds
    ) -> None:
        """Test that credentials are properly validated."""
        user, password = test_creds

        # Verify required fields are present
        assert sample_credentials.credentials_id is not None
        assert sample_credentials.name is not None
        assert sample_credentials.user is not None
        assert sample_credentials.host is not None
        assert sample_credentials.port is not None
        assert sample_credentials.database is not None

        # Verify password is encrypted
        assert sample_credentials.password != password
        assert sample_credentials.decrypted_password == password

    def test_context_credentials_management_operations(
        self, sample_context: Context, test_creds
    ) -> None:
        """Test various context credentials management operations."""
        _, password = test_creds

        # Test adding multiple credentials
        creds2 = Credentials(
            credentials_id=3,
            name="creds2",
            user="user2",
            host="localhost",
            port=1433,
            database="testdb2",
            password=password,
        )

        creds3 = Credentials(
            credentials_id=4,
            name="creds3",
            user="user3",
            host="localhost",
            port=1433,
            database="testdb3",
            password=password,
        )

        sample_context.add_credentials(creds2)
        sample_context.add_credentials(creds3)

        # Verify all credentials are accessible
        assert sample_context.get_credentials(1) is not None
        assert sample_context.get_credentials(3) == creds2
        assert sample_context.get_credentials(4) == creds3

    def test_credentials_environment_integration(
        self, sample_context: Context, test_creds
    ) -> None:
        """Test that credentials work with environment variables."""
        user, password = test_creds

        # Verify that the test credentials are properly set up
        assert user is not None
        assert password is not None

        # Verify that the context can access these credentials
        creds = sample_context.get_credentials(1)
        assert creds.user == user
        assert creds.decrypted_password == password

    def test_sqlserver_component_connection_setup(
        self, sample_context: Context, test_creds
    ) -> None:
        """Test that SQL Server components can set up connections with credentials."""
        user, password = test_creds

        # Test read component
        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = sample_context

        # Test write component
        write_comp = self._create_sqlserver_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver",
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = sample_context

        # Verify both components have access to credentials
        read_creds = read_comp._get_credentials()
        write_creds = write_comp._get_credentials()

        assert read_creds["user"] == user
        assert write_creds["user"] == user
        assert read_creds["host"] == "localhost"
        assert write_creds["host"] == "localhost"
        assert read_creds["port"] == 1433
        assert write_creds["port"] == 1433


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
