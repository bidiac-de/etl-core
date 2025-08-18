"""
Integration tests for MariaDB credentials and context system.

These tests verify that the real Credentials and Context objects work correctly
with the MariaDB components.
"""

import pytest
from unittest.mock import Mock, patch
from typing import Dict, Any

from src.context.credentials import Credentials
from src.context.context import Context
from src.context.environment import Environment
from src.context.context_parameter import ContextParameter
from src.components.databases.mariadb.mariadb_read import MariaDBRead
from src.components.schema import Schema


class TestCredentialsIntegration:
    """Test cases for real Credentials and Context integration."""

    @pytest.fixture
    def sample_credentials(self):
        """Create a real Credentials object for testing."""
        return Credentials(
            credentials_id=1,
            name="test_db_creds",
            user="testuser",
            database="testdb",
            password="testpass123",
            pool_max_size=10,
            pool_timeout_s=30
        )

    @pytest.fixture
    def sample_context(self, sample_credentials):
        """Create a real Context object with credentials."""
        context = Context(
            id=1,
            name="test_context",
            environment=Environment.TEST,
            parameters={
                "db_host": ContextParameter(
                    id=1,
                    key="db_host", 
                    value="localhost",
                    type="string",
                    is_secure=False
                ),
                "db_port": ContextParameter(
                    id=2,
                    key="db_port", 
                    value="3306",
                    type="string",
                    is_secure=False
                )
            }
        )
        # Add credentials to context
        context.add_credentials(sample_credentials)
        return context

    @pytest.fixture
    def mock_schema(self):
        """Create mock schema."""
        return Mock(spec=Schema)

    def test_credentials_creation(self, sample_credentials):
        """Test that Credentials objects are created correctly."""
        assert sample_credentials.credentials_id == 1
        assert sample_credentials.name == "test_db_creds"
        assert sample_credentials.user == "testuser"
        assert sample_credentials.database == "testdb"
        assert sample_credentials.decrypted_password == "testpass123"
        assert sample_credentials.pool_max_size == 10
        assert sample_credentials.pool_timeout_s == 30

    def test_credentials_get_parameter(self, sample_credentials):
        """Test that Credentials.get_parameter works correctly."""
        assert sample_credentials.get_parameter("user") == "testuser"
        assert sample_credentials.get_parameter("database") == "testdb"
        assert sample_credentials.get_parameter("pool_max_size") == 10
        assert sample_credentials.get_parameter("pool_timeout_s") == 30
        
        # Test invalid parameter
        with pytest.raises(KeyError, match="Unknown parameter key: invalid_key"):
            sample_credentials.get_parameter("invalid_key")

    def test_context_credentials_management(self, sample_context, sample_credentials):
        """Test that Context can store and retrieve credentials."""
        # Test getting credentials
        retrieved_creds = sample_context.get_credentials(1)
        assert retrieved_creds == sample_credentials
        
        # Test getting non-existent credentials
        with pytest.raises(KeyError, match="Credentials with ID 2 not found"):
            sample_context.get_credentials(2)

    def test_context_add_credentials(self, sample_context):
        """Test adding new credentials to context."""
        new_creds = Credentials(
            credentials_id=2,
            name="another_db",
            user="user2",
            database="db2",
            password="pass2"
        )
        
        sample_context.add_credentials(new_creds)
        
        # Verify it was added
        retrieved = sample_context.get_credentials(2)
        assert retrieved == new_creds
        assert retrieved.user == "user2"

    def test_mariadb_component_with_real_credentials(self, sample_context, mock_schema):
        """Test that MariaDBRead works with real credentials."""
        read_comp = MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="database",
            schema=mock_schema,
            database="testdb",
            table="users",
            query="SELECT * FROM users",
            host="localhost",
            port=3306,
            credentials_id=1
        )
        
        # Set the context
        read_comp.context = sample_context
        
        # Test that credentials can be retrieved
        creds = read_comp._get_credentials()
        assert creds["user"] == "testuser"
        assert creds["password"] == "testpass123"
        assert creds["database"] == "testdb"

    def test_credentials_pool_parameters(self, sample_credentials):
        """Test that pool parameters are correctly exposed."""
        # Test pool parameters are accessible
        assert sample_credentials.get_parameter("pool_max_size") == 10
        assert sample_credentials.get_parameter("pool_timeout_s") == 30
        
        # Test that these can be used by pool_args
        from src.components.databases.pool_args import build_sql_engine_kwargs
        
        engine_kwargs = build_sql_engine_kwargs(sample_credentials)
        assert engine_kwargs["pool_size"] == 10
        assert engine_kwargs["pool_timeout"] == 30

    def test_credentials_without_pool_settings(self):
        """Test credentials without pool settings."""
        creds = Credentials(
            credentials_id=3,
            name="minimal_creds",
            user="minuser",
            database="mindb",
            password="minpass"
        )
        
        # Test that pool parameters return None
        assert creds.get_parameter("pool_max_size") is None
        assert creds.get_parameter("pool_timeout_s") is None
        
        # Test pool_args with minimal credentials
        from src.components.databases.pool_args import build_sql_engine_kwargs
        
        engine_kwargs = build_sql_engine_kwargs(creds)
        # Should be empty dict when no pool settings
        assert engine_kwargs == {}

    def test_credentials_password_handling(self):
        """Test password handling in credentials."""
        # Test with password
        creds_with_pass = Credentials(
            credentials_id=4,
            name="with_pass",
            user="user4",
            database="db4",
            password="secret123"
        )
        assert creds_with_pass.decrypted_password == "secret123"
        
        # Test without password
        creds_no_pass = Credentials(
            credentials_id=5,
            name="no_pass",
            user="user5",
            database="db5"
        )
        assert creds_no_pass.decrypted_password is None

    def test_context_parameter_retrieval(self, sample_context):
        """Test that Context.get_parameter works for regular parameters."""
        assert sample_context.get_parameter("db_host") == "localhost"
        assert sample_context.get_parameter("db_port") == "3306"
        
        # Test non-existent parameter
        with pytest.raises(KeyError, match="Parameter with key 'invalid_param' not found"):
            sample_context.get_parameter("invalid_param")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
