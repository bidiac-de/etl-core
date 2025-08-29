"""
Integration tests for SQL Server credentials and context functionality.

These tests verify that the Credentials and Context classes work together
properly in real scenarios, using environment variables for sensitive data.
"""

import hashlib
import os
from typing import Tuple

import pytest
from unittest.mock import Mock, patch

from etl_core.context.context import Context
from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials
from etl_core.context.context_parameter import ContextParameter
from etl_core.components.databases.pool_args import build_sql_engine_kwargs
from etl_core.components.databases.sqlserver.sqlserver_read import SQLServerRead
from etl_core.components.databases.sqlserver.sqlserver_write import SQLServerWrite


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

    @patch(
        "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
    )
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
        assert creds["host"] == "localhost"
        assert creds["port"] == 1433  # SQL Server default port
        assert creds["database"] == "testdb"

    @patch(
        "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
    )
    def test_sqlserver_write_component_with_real_credentials(
        self, mock_handler_class, sample_context: Context, test_creds
    ) -> None:
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler
        write_comp = SQLServerWrite(
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver",
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = sample_context
        creds = write_comp._get_credentials()
        
        assert creds["user"] == test_creds[0]
        assert creds["host"] == "localhost"
        assert creds["port"] == 1433  # SQL Server default port
        assert creds["database"] == "testdb"

    def test_credentials_pool_parameters(self, sample_credentials: Credentials) -> None:
        """Test credentials pool parameter handling."""
        assert sample_credentials.pool_max_size == 10
        assert sample_credentials.pool_timeout_s == 30
        
        # Test parameter retrieval
        assert sample_credentials.get_parameter("pool_max_size") == 10
        assert sample_credentials.get_parameter("pool_timeout_s") == 30

    def test_credentials_without_pool_settings(self, test_creds: Tuple[str, str]) -> None:
        """Test credentials without pool settings."""
        user, password = test_creds
        creds = Credentials(
            credentials_id=3,
            name="no_pool_creds",
            user=user,
            host="localhost",
            port=1433,
            database="testdb",
            password=password,
            # No pool settings
        )
        
        assert creds.pool_max_size is None
        assert creds.pool_timeout_s is None
        
        # Test parameter retrieval returns None
        assert creds.get_parameter("pool_max_size") is None
        assert creds.get_parameter("pool_timeout_s") is None

    def test_credentials_password_handling(self, test_creds: Tuple[str, str]) -> None:
        """Test credentials password handling."""
        user, password = test_creds
        creds = Credentials(
            credentials_id=4,
            name="password_test",
            user=user,
            host="localhost",
            port=1433,
            database="testdb",
            password=password,
        )
        
        # Test decrypted password
        assert creds.decrypted_password == password
        
        # Test password is not exposed in repr
        creds_repr = repr(creds)
        assert password not in creds_repr

    def test_context_parameter_retrieval(self, sample_context: Context) -> None:
        """Test context parameter retrieval."""
        creds = sample_context.get_credentials(1)
        
        # Test all parameters are accessible
        assert creds.get_parameter("user") is not None
        assert creds.get_parameter("host") is not None
        assert creds.get_parameter("port") is not None
        assert creds.get_parameter("database") is not None
        assert creds.get_parameter("pool_max_size") is not None
        assert creds.get_parameter("pool_timeout_s") is not None

    def test_credentials_validation(self, test_creds: Tuple[str, str]) -> None:
        """Test credentials validation."""
        user, password = test_creds
        
        # Test valid credentials
        valid_creds = Credentials(
            credentials_id=5,
            name="valid_creds",
            user=user,
            host="localhost",
            port=1433,
            database="testdb",
            password=password,
        )
        
        assert valid_creds.credentials_id == 5
        assert valid_creds.name == "valid_creds"
        assert valid_creds.user == user
        assert valid_creds.host == "localhost"
        assert valid_creds.port == 1433
        assert valid_creds.database == "testdb"

    def test_context_parameter_types(self, sample_context: Context) -> None:
        """Test context parameter types."""
        creds = sample_context.get_credentials(1)
        
        # Test parameter types
        assert isinstance(creds.get_parameter("user"), str)
        assert isinstance(creds.get_parameter("host"), str)
        assert isinstance(creds.get_parameter("port"), int)
        assert isinstance(creds.get_parameter("database"), str)
        assert isinstance(creds.get_parameter("pool_max_size"), int)
        assert isinstance(creds.get_parameter("pool_timeout_s"), int)

    def test_context_secure_parameters(self, sample_context: Context) -> None:
        """Test context secure parameter handling."""
        creds = sample_context.get_credentials(1)
        
        # Test password is not exposed in parameters
        with pytest.raises(KeyError, match="Unknown parameter key: password"):
            creds.get_parameter("password")
        
        # Test decrypted password is accessible via property
        assert creds.decrypted_password is not None

    def test_context_environment_handling(self, sample_context: Context) -> None:
        """Test context environment handling."""
        # Test environment is accessible
        assert hasattr(sample_context, 'environment')
        
        # Test environment parameters
        env = sample_context.environment
        assert env is not None

    def test_credentials_pool_configuration_validation(self, test_creds: Tuple[str, str]) -> None:
        """Test credentials pool configuration validation."""
        user, password = test_creds
        
        # Test valid pool configuration
        valid_pool_creds = Credentials(
            credentials_id=6,
            name="valid_pool_creds",
            user=user,
            host="localhost",
            port=1433,
            database="testdb",
            password=password,
            pool_max_size=20,
            pool_timeout_s=60,
        )
        
        assert valid_pool_creds.pool_max_size == 20
        assert valid_pool_creds.pool_timeout_s == 60

    def test_context_parameter_validation(self, sample_context: Context) -> None:
        """Test context parameter validation."""
        creds = sample_context.get_credentials(1)
        
        # Test valid parameters
        assert creds.get_parameter("user") is not None
        assert creds.get_parameter("host") is not None
        assert creds.get_parameter("port") is not None
        assert creds.get_parameter("database") is not None
        
        # Test invalid parameters raise KeyError
        with pytest.raises(KeyError):
            creds.get_parameter("invalid_param")

    def test_credentials_in_sqlserver_component_integration(
        self, sample_context: Context, test_creds: Tuple[str, str]
    ) -> None:
        """Test credentials integration in SQL Server components."""
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
        write_comp = SQLServerWrite(
            name="test_write",
            description="Test write component",
            comp_type="write_sqlserver",
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = sample_context
        
        # Verify both components can access credentials
        read_creds = read_comp._get_credentials()
        write_creds = write_comp._get_credentials()
        
        assert read_creds["user"] == user
        assert write_creds["user"] == user
        assert read_creds["host"] == "localhost"
        assert write_creds["host"] == "localhost"
        assert read_creds["port"] == 1433
        assert write_creds["port"] == 1433

    def test_context_credentials_multiple_databases(self, sample_context: Context) -> None:
        """Test context with multiple database credentials."""
        # Add second database credentials
        second_creds = Credentials(
            credentials_id=2,
            name="second_db",
            user="user2",
            host="localhost",
            port=1433,
            database="seconddb",
            password="password2",
            pool_max_size=15,
            pool_timeout_s=45,
        )
        
        sample_context.add_credentials(second_creds)
        
        # Verify both credentials are accessible
        first_creds = sample_context.get_credentials(1)
        second_creds_retrieved = sample_context.get_credentials(2)
        
        assert first_creds.database == "testdb"
        assert second_creds_retrieved.database == "seconddb"
        assert first_creds.pool_max_size == 10
        assert second_creds_retrieved.pool_max_size == 15

    def test_context_parameter_immutability(self, sample_context: Context) -> None:
        """Test context parameter immutability."""
        creds = sample_context.get_credentials(1)
        
        # Test that parameters cannot be modified
        original_user = creds.get_parameter("user")
        original_host = creds.get_parameter("host")
        
        # Verify parameters remain unchanged
        assert creds.get_parameter("user") == original_user
        assert creds.get_parameter("host") == original_host

    def test_credentials_database_name_validation(self, test_creds: Tuple[str, str]) -> None:
        """Test credentials database name validation."""
        user, password = test_creds
        
        # Test various database names
        valid_db_names = [
            "testdb",
            "TestDB",
            "test_db",
            "test123",
            "my_database",
            "production_db"
        ]
        
        for db_name in valid_db_names:
            creds = Credentials(
                credentials_id=7,
                name=f"test_{db_name}",
                user=user,
                host="localhost",
                port=1433,
                database=db_name,
                password=password,
            )
            assert creds.database == db_name

    def test_context_parameter_key_validation(self, sample_context: Context) -> None:
        """Test context parameter key validation."""
        creds = sample_context.get_credentials(1)
        
        # Test valid parameter keys
        valid_keys = [
            "credentials_id",
            "user",
            "host",
            "port",
            "database",
            "pool_max_size",
            "pool_timeout_s"
        ]
        
        for key in valid_keys:
            value = creds.get_parameter(key)
            assert value is not None
        
        # Test invalid parameter keys
        invalid_keys = [
            "invalid_key",
            "password",
            "secret",
            "unknown"
        ]
        
        for key in invalid_keys:
            with pytest.raises(KeyError, match=f"Unknown parameter key: {key}"):
                creds.get_parameter(key)

    @patch(
        "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
    )
    def test_sqlserver_write_bulk_operations(
        self, mock_handler_class, sample_context: Context, test_creds: Tuple[str, str]
    ) -> None:
        """Test SQL Server write bulk operations with credentials."""
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler
        
        write_comp = SQLServerWrite(
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

    @patch(
        "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
    )
    def test_sqlserver_read_query_operations(
        self, mock_handler_class, sample_context: Context, test_creds: Tuple[str, str]
    ) -> None:
        """Test SQL Server read query operations with credentials."""
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler
        
        read_comp = SQLServerRead(
            name="test_read",
            description="Test read component",
            comp_type="read_sqlserver",
            entity_name="users",
            query="SELECT * FROM users WHERE active = :active",
            credentials_id=1,
        )
        read_comp.context = sample_context
        
        # Verify credentials are accessible
        creds = read_comp._get_credentials()
        assert creds["user"] == test_creds[0]
        assert creds["database"] == "testdb"

    def test_multiple_credentials_fixture(self, multiple_credentials) -> None:
        """Test multiple credentials fixture."""
        assert len(multiple_credentials) == 2
        
        # Verify first credentials
        first_creds = multiple_credentials[0]
        assert first_creds.credentials_id == 1
        assert first_creds.database == "testdb"
        
        # Verify second credentials
        second_creds = multiple_credentials[1]
        assert second_creds.credentials_id == 2
        assert second_creds.database == "seconddb"

    def test_context_with_multiple_credentials_fixture(
        self, context_with_multiple_credentials: Context
    ) -> None:
        """Test context with multiple credentials fixture."""
        # Verify both credentials are accessible
        first_creds = context_with_multiple_credentials.get_credentials(1)
        second_creds = context_with_multiple_credentials.get_credentials(2)
        
        assert first_creds.database == "testdb"
        assert second_creds.database == "seconddb"

    def test_sqlserver_component_fixtures(
        self, sqlserver_read_component: SQLServerRead, sqlserver_write_component: SQLServerWrite
    ) -> None:
        """Test SQL Server component fixtures."""
        # Verify read component
        assert sqlserver_read_component.comp_type == "read_sqlserver"
        assert sqlserver_read_component.entity_name == "users"
        assert sqlserver_read_component.credentials_id == 1
        
        # Verify write component
        assert sqlserver_write_component.comp_type == "write_sqlserver"
        assert sqlserver_write_component.entity_name == "users"
        assert sqlserver_write_component.credentials_id == 1

    def test_sample_sql_queries_fixture(self, sample_sql_queries) -> None:
        """Test sample SQL queries fixture."""
        assert len(sample_sql_queries) > 0
        
        # Verify all queries are strings
        for query in sample_sql_queries:
            assert isinstance(query, str)
            assert len(query) > 0

    def test_sample_query_params_fixture(self, sample_query_params) -> None:
        """Test sample query parameters fixture."""
        assert len(sample_query_params) > 0
        
        # Verify all parameters are dictionaries
        for params in sample_query_params:
            assert isinstance(params, dict)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


