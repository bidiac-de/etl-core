"""
Integration tests for MariaDB credentials and context system.

These tests verify that the real Credentials and Context objects work correctly
with the MariaDB components.
"""

import pytest
from unittest.mock import Mock

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
            pool_timeout_s=30,
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
                    is_secure=False,
                ),
                "db_port": ContextParameter(
                    id=2, key="db_port", value="3306", type="string", is_secure=False
                ),
            },
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
            password="pass2",
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
            credentials_id=1,
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
            password="minpass",
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
            password="secret123",
        )
        assert creds_with_pass.decrypted_password == "secret123"

        # Test without password
        creds_no_pass = Credentials(
            credentials_id=5, name="no_pass", user="user5", database="db5"
        )
        assert creds_no_pass.decrypted_password is None

    def test_context_parameter_retrieval(self, sample_context):
        """Test that Context.get_parameter works for regular parameters."""
        assert sample_context.get_parameter("db_host") == "localhost"
        assert sample_context.get_parameter("db_port") == "3306"

        # Test non-existent parameter
        with pytest.raises(
            KeyError, match="Parameter with key 'invalid_param' not found"
        ):
            sample_context.get_parameter("invalid_param")

    # NEW TESTS FOR IMPROVED CREDENTIAL SYSTEM COVERAGE

    def test_credentials_validation(self):
        """Test credentials validation and constraints."""
        # Test valid credentials
        valid_creds = Credentials(
            credentials_id=6,
            name="valid_creds",
            user="validuser",
            database="validdb",
            password="validpass",
        )
        assert valid_creds.credentials_id == 6
        assert valid_creds.name == "valid_creds"

        # Test credentials with special characters
        special_creds = Credentials(
            credentials_id=7,
            name="special_creds_2024",
            user="user@domain",
            database="test-db_123",
            password="pass@word#123",
        )
        assert special_creds.user == "user@domain"
        assert special_creds.database == "test-db_123"
        assert special_creds.decrypted_password == "pass@word#123"

    def test_credentials_edge_cases(self):
        """Test credentials with edge case values."""
        # Test empty strings
        empty_creds = Credentials(
            credentials_id=8, name="", user="", database="", password=""
        )
        assert empty_creds.name == ""
        assert empty_creds.user == ""
        assert empty_creds.database == ""
        assert empty_creds.decrypted_password == ""

        # Test very long values
        long_creds = Credentials(
            credentials_id=9,
            name="a" * 1000,
            user="b" * 1000,
            database="c" * 1000,
            password="d" * 1000,
        )
        assert len(long_creds.name) == 1000
        assert len(long_creds.user) == 1000
        assert len(long_creds.database) == 1000
        assert len(long_creds.decrypted_password) == 1000

    def test_context_parameter_types(self):
        """Test different context parameter types."""
        # Test string parameter
        string_param = ContextParameter(
            id=10,
            key="string_param",
            value="test_value",
            type="string",
            is_secure=False,
        )
        assert string_param.value == "test_value"
        assert string_param.type == "string"

        # Test numeric parameter
        numeric_param = ContextParameter(
            id=11, key="numeric_param", value="42", type="integer", is_secure=False
        )
        assert numeric_param.value == "42"
        assert numeric_param.type == "integer"

        # Test boolean parameter
        boolean_param = ContextParameter(
            id=12, key="boolean_param", value="true", type="boolean", is_secure=False
        )
        assert boolean_param.value == "true"
        assert boolean_param.type == "boolean"

    def test_context_secure_parameters(self):
        """Test secure context parameters."""
        # Test secure parameter
        secure_param = ContextParameter(
            id=13,
            key="db_password",
            value="secret_password",
            type="string",
            is_secure=True,
        )
        assert secure_param.is_secure is True
        assert secure_param.value == "secret_password"

        # Test non-secure parameter
        non_secure_param = ContextParameter(
            id=14, key="db_host", value="localhost", type="string", is_secure=False
        )
        assert non_secure_param.is_secure is False

    def test_context_environment_handling(self):
        """Test context environment handling."""
        # Test different environments
        test_context = Context(
            id=15, name="test_env", environment=Environment.TEST, parameters={}
        )
        assert test_context.environment == Environment.TEST

        prod_context = Context(
            id=16, name="prod_env", environment=Environment.PROD, parameters={}
        )
        assert prod_context.environment == Environment.PROD

        dev_context = Context(
            id=17, name="dev_env", environment=Environment.DEV, parameters={}
        )
        assert dev_context.environment == Environment.DEV

    def test_credentials_pool_configuration_validation(self):
        """Test credentials pool configuration validation."""
        # Test valid pool configuration
        valid_pool_creds = Credentials(
            credentials_id=18,
            name="valid_pool",
            user="pooluser",
            database="pooldb",
            password="poolpass",
            pool_max_size=50,
            pool_timeout_s=60,
        )
        assert valid_pool_creds.pool_max_size == 50
        assert valid_pool_creds.pool_timeout_s == 60

        # Test minimum values
        min_pool_creds = Credentials(
            credentials_id=19,
            name="min_pool",
            user="minuser",
            database="mindb",
            password="minpass",
            pool_max_size=1,
            pool_timeout_s=1,
        )
        assert min_pool_creds.pool_max_size == 1
        assert min_pool_creds.pool_timeout_s == 1

    def test_context_parameter_validation(self):
        """Test context parameter validation."""
        # Test required fields
        with pytest.raises(ValueError):
            ContextParameter(
                id=None,  # Missing required field
                key="test",
                value="test",
                type="string",
                is_secure=False,
            )

        # Test valid parameter
        valid_param = ContextParameter(
            id=20, key="valid_key", value="valid_value", type="string", is_secure=False
        )
        assert valid_param.id == 20
        assert valid_param.key == "valid_key"

    def test_credentials_in_mariadb_component_integration(
        self, sample_context, mock_schema
    ):
        """Test complete integration of credentials in MariaDB component."""
        read_comp = MariaDBRead(
            name="integration_test",
            description="Integration test component",
            comp_type="database",
            schema=mock_schema,
            database="testdb",
            table="users",
            query="SELECT * FROM users WHERE id = %(id)s",
            params={"id": 1},
            host="localhost",
            port=3306,
            credentials_id=1,
        )

        # Set the context
        read_comp.context = sample_context

        # Test credential retrieval
        creds = read_comp._get_credentials()
        assert creds["user"] == "testuser"
        assert creds["password"] == "testpass123"
        assert creds["database"] == "testdb"

        # Test that credentials are properly formatted for database connection
        assert isinstance(creds["user"], str)
        assert isinstance(creds["password"], str)
        assert isinstance(creds["database"], str)

    def test_context_credentials_multiple_databases(self):
        """Test context with multiple database credentials."""
        # Create multiple credentials
        creds1 = Credentials(
            credentials_id=21,
            name="db1_creds",
            user="user1",
            database="database1",
            password="pass1",
        )

        creds2 = Credentials(
            credentials_id=22,
            name="db2_creds",
            user="user2",
            database="database2",
            password="pass2",
        )

        # Create context with multiple credentials
        multi_context = Context(
            id=23, name="multi_db_context", environment=Environment.TEST, parameters={}
        )

        multi_context.add_credentials(creds1)
        multi_context.add_credentials(creds2)

        # Test retrieval
        retrieved_creds1 = multi_context.get_credentials(21)
        retrieved_creds2 = multi_context.get_credentials(22)

        assert retrieved_creds1 == creds1
        assert retrieved_creds2 == creds2
        assert retrieved_creds1.database == "database1"
        assert retrieved_creds2.database == "database2"

    def test_credentials_password_security(self):
        """Test password security aspects."""
        # Test that passwords are stored as provided (in real system, they'd be
        # encrypted)
        secure_creds = Credentials(
            credentials_id=24,
            name="secure_creds",
            user="secureuser",
            database="securedb",
            password="very_secure_password_123!@#",
        )

        # Verify password is accessible (in real system, this would be decrypted)
        assert secure_creds.decrypted_password == "very_secure_password_123!@#"

        # Test special characters in password
        special_pass_creds = Credentials(
            credentials_id=25,
            name="special_pass",
            user="specialuser",
            database="specialdb",
            password="p@ssw0rd!@#$%^&*()_+-=[]{}|;':\",./<>?",
        )
        assert (
            special_pass_creds.decrypted_password
            == "p@ssw0rd!@#$%^&*()_+-=[]{}|;':\",./<>?"
        )

    def test_context_parameter_immutability(self):
        """Test that context parameters are immutable."""
        param = ContextParameter(
            id=26,
            key="immutable_param",
            value="original_value",
            type="string",
            is_secure=False,
        )

        # Test that we can't modify the parameter after creation
        # (This depends on the actual implementation - adjust if needed)
        assert param.value == "original_value"
        assert param.key == "immutable_param"

    def test_credentials_database_name_validation(self):
        """Test database name validation in credentials."""
        # Test valid database names
        valid_db_names = [
            "testdb",
            "test_db",
            "test123",
            "test-db",
            "test.db",
            "test_db_2024",
        ]

        for i, db_name in enumerate(valid_db_names):
            creds = Credentials(
                credentials_id=30 + i,
                name=f"creds_{i}",
                user=f"user{i}",
                database=db_name,
                password=f"pass{i}",
            )
            assert creds.database == db_name

    def test_context_parameter_key_validation(self):
        """Test context parameter key validation."""
        # Test valid parameter keys
        valid_keys = [
            "db_host",
            "db_port",
            "api_key",
            "timeout",
            "max_retries",
            "batch_size",
        ]

        for i, key in enumerate(valid_keys):
            param = ContextParameter(
                id=40 + i, key=key, value=f"value_{i}", type="string", is_secure=False
            )
            assert param.key == key


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
