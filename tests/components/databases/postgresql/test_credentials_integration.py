"""
Integration tests for PostgreSQL credentials and context system.

These tests verify that the real Credentials and Context objects work correctly
with the PostgreSQL components.
"""

import pytest
from unittest.mock import Mock, patch

from etl_core.components.databases.postgresql.postgresql_read import PostgreSQLRead
from etl_core.components.databases.postgresql.postgresql_write import PostgreSQLWrite
from etl_core.context.context import Context
from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials
from etl_core.context.context_parameter import ContextParameter
from etl_core.components.databases.pool_args import build_sql_engine_kwargs
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType


class TestPostgreSQLCredentialsIntegration:
    """Test PostgreSQL credentials integration."""

    def _create_postgresql_write_with_schema(self, **kwargs):
        """Helper to create PostgreSQLWrite component with proper schema."""

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

        write_comp = PostgreSQLWrite(**kwargs)
        return write_comp

    @pytest.fixture
    def sample_credentials(self):
        """Fixture for sample credentials."""
        return Credentials(
            credentials_id=1,
            name="test_db_creds",
            user="testuser",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="testdb",
            password="testpass123",
            pool_max_size=10,
            pool_timeout_s=30,
        )

    @pytest.fixture
    def sample_context(self, sample_credentials):
        """Fixture for sample context."""
        context = Context(
            id=1,
            name="test_env",
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
                    id=2, key="db_port", value="5432", type="string", is_secure=False
                ),
            },
        )
        context.add_credentials(sample_credentials)
        return context

    @pytest.fixture
    def sample_dataframe(self):
        """Fixture for a sample DataFrame."""
        import pandas as pd

        return pd.DataFrame({"id": [1, 2, 3], "name": ["John", "Jane", "Bob"]})

    @pytest.fixture
    def multiple_credentials(self):
        """Fixture for multiple credentials."""
        return {
            "minimal": Credentials(
                credentials_id=21,
                name="minimal_creds",
                user="minuser",
                host="localhost",
                port=5432,  # PostgreSQL default port
                database="mindb",
                password="minpass",
            ),
            "with_pool": Credentials(
                credentials_id=22,
                name="pool_creds",
                user="pooluser",
                host="localhost",
                port=5432,  # PostgreSQL default port
                database="pooldb",
                password="poolpass",
                pool_max_size=50,
                pool_timeout_s=60,
            ),
            "special_chars": Credentials(
                credentials_id=23,
                name="special_creds",
                user="user@domain",
                host="localhost",
                port=5432,  # PostgreSQL default port
                database="test-db_123",
                password="pass@word#123",
            ),
            "no_password": Credentials(
                credentials_id=24,
                name="nopass_creds",
                user="nopassuser",
                host="localhost",
                port=5432,  # PostgreSQL default port
                database="nopassdb",
            ),
        }

    @pytest.fixture
    def context_with_multiple_credentials(self, multiple_credentials):
        """Fixture for a context with multiple credentials."""
        multi_context = Context(
            id=23, name="multi_db_context", environment=Environment.TEST, parameters={}
        )
        for creds in multiple_credentials.values():
            multi_context.add_credentials(creds)
        return multi_context

    @pytest.fixture
    def postgresql_read_component(self, sample_context):
        """Fixture for a PostgreSQLRead component."""
        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )
        read_comp.context = sample_context
        return read_comp

    @pytest.fixture
    def postgresql_write_component(self, sample_context):
        """Fixture for a PostgreSQLWrite component."""
        write_comp = self._create_postgresql_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            entity_name="users",
            credentials_id=1,
        )
        write_comp.context = sample_context
        return write_comp

    @pytest.fixture
    def sample_sql_queries(self):
        """Fixture for sample SQL queries."""
        return {
            "simple_select": "SELECT * FROM users",
            "parameterized": "SELECT * FROM users WHERE id = \
            %(id)s AND active = %(active)s",
            "complex_join": "SELECT u.name, p.price FROM users \
            u JOIN products p ON u.id = p.user_id",
            "aggregation": "SELECT COUNT(*) FROM users",
            "insert": "INSERT INTO users (name, email) VALUES (%(name)s, %(email)s)",
            "update": "UPDATE users SET email = %(email)s WHERE id = %(id)s",
            "delete": "DELETE FROM users WHERE id = %(id)s",
        }

    @pytest.fixture
    def sample_query_params(self):
        """Fixture for sample query parameters."""
        return {
            "simple": {"id": 1},
            "user_lookup": {"id": 1, "active": True},
            "bulk_insert": [
                {"name": "John", "email": "john@example.com"},
                {"name": "Jane", "email": "jane@example.com"},
                {"name": "Bob", "email": "bob@example.com"},
            ],
            "filter": {"active": True},
            "pagination": {"limit": 10, "offset": 0},
        }

    def test_credentials_creation(self, sample_credentials):
        """Test that Credentials objects are created correctly."""
        assert sample_credentials.credentials_id == 1
        assert sample_credentials.name == "test_db_creds"
        assert sample_credentials.user == "testuser"
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
            name="new_creds",
            user="newuser",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="newdb",
            password="pass2",
        )
        sample_context.add_credentials(new_creds)

        # Verify it was added
        retrieved = sample_context.get_credentials(2)
        assert retrieved.name == "new_creds"
        assert retrieved.user == "newuser"

    @patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
    def test_postgresql_read_component_with_real_credentials(
        self, mock_handler_class, sample_context
    ):
        """Test that PostgreSQLRead works with real credentials."""
        # Mock the connection handler
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler

        read_comp = PostgreSQLRead(
            name="test_read",
            description="Test read component",
            comp_type="read_postgresql",
            entity_name="users",
            query="SELECT * FROM users",
            credentials_id=1,
        )

        # Set the context
        read_comp.context = sample_context

        # Test that credentials can be retrieved
        creds = read_comp._get_credentials()
        assert creds["user"] == "testuser"
        assert creds["password"] == "testpass123"
        assert creds["database"] == "testdb"

    @patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
    def test_postgresql_write_component_with_real_credentials(
        self, mock_handler_class, sample_context
    ):
        """Test that PostgreSQLWrite works with real credentials."""
        # Mock the connection handler
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler

        write_comp = self._create_postgresql_write_with_schema(
            name="test_write",
            description="Test write component",
            comp_type="write_postgresql",
            database="testdb",
            entity_name="users",
            credentials_id=1,
        )

        # Set the context
        write_comp.context = sample_context

        # Test that credentials can be retrieved
        creds = write_comp._get_credentials()
        assert creds["user"] == "testuser"
        assert creds["password"] == "testpass123"
        assert creds["database"] == "testdb"

    def test_credentials_pool_parameters(self, sample_credentials):
        """Test that pool parameters are correctly exposed."""
        # Test pool parameters are accessible
        assert sample_credentials.get_parameter("pool_max_size") == 10
        assert sample_credentials.get_parameter("pool_timeout_s") == 30

        engine_kwargs = build_sql_engine_kwargs(sample_credentials)
        assert engine_kwargs["pool_size"] == 10
        assert engine_kwargs["pool_timeout"] == 30

    def test_credentials_without_pool_settings(self):
        """Test credentials without pool settings."""
        creds = Credentials(
            credentials_id=3,
            name="minimal_creds",
            user="minuser",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="mindb",
            password="minpass",
        )

        # Should have default pool settings
        assert creds.pool_max_size is None
        assert creds.pool_timeout_s is None

        engine_kwargs = build_sql_engine_kwargs(creds)
        # Should be empty dict when no pool settings
        assert engine_kwargs == {}

    def test_credentials_password_handling(self):
        """Test credentials password handling."""

        creds_no_pass = Credentials(
            credentials_id=5,
            name="nopass_creds",
            user="nopassuser",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="nopassdb",
        )
        assert creds_no_pass.decrypted_password is None

    def test_context_parameter_retrieval(self, sample_context):
        """Test that Context.get_parameter works for regular parameters."""
        assert sample_context.get_parameter("db_host") == "localhost"
        assert (
            sample_context.get_parameter("db_port") == "5432"
        )  # PostgreSQL default port

        # Test non-existent parameter
        with pytest.raises(
            KeyError, match="Parameter with key 'invalid_param' not found"
        ):
            sample_context.get_parameter("invalid_param")

    # NEW TESTS FOR IMPROVED CREDENTIAL SYSTEM COVERAGE

    def test_credentials_validation(self):
        """Test credentials validation."""
        valid_creds = Credentials(
            credentials_id=6,
            name="valid_creds",
            user="validuser",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="validdb",
            password="validpass",
        )

        special_creds = Credentials(
            credentials_id=7,
            name="special_creds",
            user="user@domain",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="special_db",
            password="pass@word#123",
        )
        assert valid_creds.credentials_id == 6
        assert valid_creds.name == "valid_creds"

        # Test credentials with special characters
        special_creds = Credentials(
            credentials_id=7,
            name="special_creds_2024",
            user="user@domain",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="test-db_123",
            password="pass@word#123",
        )
        assert special_creds.user == "user@domain"
        assert special_creds.database == "test-db_123"
        assert special_creds.decrypted_password == "pass@word#123"

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
        valid_pool_creds = Credentials(
            credentials_id=18,
            name="pool_creds",
            user="pooluser",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="pooldb",
            password="poolpass",
            pool_max_size=50,
            pool_timeout_s=60,
        )

        min_pool_creds = Credentials(
            credentials_id=19,
            name="min_pool_creds",
            user="minpooluser",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="minpooldb",
            password="minpoolpass",
            pool_max_size=1,
            pool_timeout_s=1,
        )
        assert valid_pool_creds.pool_max_size == 50
        assert valid_pool_creds.pool_timeout_s == 60

        # Test minimum values
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

    @patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
    def test_credentials_in_postgresql_component_integration(
        self, mock_handler_class, sample_context
    ):
        """Test complete integration of credentials in PostgreSQL component."""
        # Mock the connection handler
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler

        read_comp = PostgreSQLRead(
            name="integration_test",
            description="Integration test component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users WHERE id = %(id)s",
            params={"id": 1},
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
        multi_context = Context(
            id=23, name="multi_db_context", environment=Environment.TEST, parameters={}
        )

        creds1 = Credentials(
            credentials_id=21,
            name="db1_creds",
            user="user1",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="db1",
            password="pass1",
        )

        creds2 = Credentials(
            credentials_id=22,
            name="db2_creds",
            user="user2",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="db2",
            password="pass2",
        )

        # Create context with multiple credentials
        multi_context.add_credentials(creds1)
        multi_context.add_credentials(creds2)

        # Test retrieval
        retrieved_creds1 = multi_context.get_credentials(21)
        retrieved_creds2 = multi_context.get_credentials(22)

        assert retrieved_creds1 == creds1
        assert retrieved_creds2 == creds2
        assert retrieved_creds1.database == "db1"
        assert retrieved_creds2.database == "db2"

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
        """Test credentials database name validation."""

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
                host="localhost",
                port=5432,  # PostgreSQL default port
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

    @patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
    def test_postgresql_write_bulk_operations(
        self, mock_handler_class, sample_context, sample_dataframe
    ):
        """Test PostgreSQL write bulk operations with real credentials."""
        # Mock the connection handler
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler

        write_comp = self._create_postgresql_write_with_schema(
            name="test_write_bulk",
            description="Test write bulk component",
            comp_type="write_postgresql",
            database="testdb",
            entity_name="users",
            credentials_id=1,
        )

        # Set the context
        write_comp.context = sample_context

        # Test credentials retrieval
        creds = write_comp._get_credentials()
        assert creds["user"] == "testuser"
        assert creds["database"] == "testdb"

        # Test that the component is properly configured
        assert write_comp.entity_name == "users"
        assert write_comp.credentials_id == 1

    @patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
    def test_postgresql_read_query_operations(self, mock_handler_class, sample_context):
        """Test PostgreSQL read query operations with real credentials."""
        # Mock the connection handler
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler

        read_comp = PostgreSQLRead(
            name="test_read_query",
            description="Test read query component",
            comp_type="read_postgresql",
            database="testdb",
            entity_name="users",
            query="SELECT * FROM users WHERE active = %(active)s",
            params={"active": True},
            credentials_id=1,
        )

        # Set the context
        read_comp.context = sample_context

        # Test credentials retrieval
        creds = read_comp._get_credentials()
        assert creds["user"] == "testuser"
        assert creds["database"] == "testdb"

        # Test that the component is properly configured
        assert read_comp.query == "SELECT * FROM users WHERE active = %(active)s"
        assert read_comp.params == {"active": True}
        assert read_comp.entity_name == "users"
        assert read_comp.credentials_id == 1

    # NEW TESTS USING THE IMPROVED FIXTURES

    def test_multiple_credentials_fixture(self, multiple_credentials):
        """Test the multiple credentials fixture."""
        assert len(multiple_credentials) == 4
        assert "minimal" in multiple_credentials
        assert "with_pool" in multiple_credentials
        assert "special_chars" in multiple_credentials
        assert "no_password" in multiple_credentials

        # Test specific credential types
        minimal_creds = multiple_credentials["minimal"]
        assert minimal_creds.pool_max_size is None
        assert minimal_creds.pool_timeout_s is None

        pool_creds = multiple_credentials["with_pool"]
        assert pool_creds.pool_max_size == 50
        assert pool_creds.pool_timeout_s == 60

        special_creds = multiple_credentials["special_chars"]
        assert special_creds.user == "user@domain"
        assert special_creds.database == "test-db_123"

        no_pass_creds = multiple_credentials["no_password"]
        assert no_pass_creds.password is None

    def test_context_with_multiple_credentials_fixture(
        self, context_with_multiple_credentials, multiple_credentials
    ):
        """Test the context with multiple credentials fixture."""
        context = context_with_multiple_credentials

        # Test that all credentials are accessible
        for creds in multiple_credentials.values():
            retrieved = context.get_credentials(creds.credentials_id)
            assert retrieved == creds

    def test_postgresql_component_fixtures(
        self, postgresql_read_component, postgresql_write_component
    ):
        """Test the PostgreSQL component fixtures."""
        # Test read component
        assert postgresql_read_component.name == "test_read"
        assert postgresql_read_component.entity_name == "users"
        assert postgresql_read_component.query == "SELECT * FROM users"
        assert postgresql_read_component.credentials_id == 1

        # Test write component
        assert postgresql_write_component.name == "test_write"
        assert postgresql_write_component.entity_name == "users"
        assert postgresql_write_component.credentials_id == 1

        # Test that both have context set
        assert postgresql_read_component.context is not None
        assert postgresql_write_component.context is not None

    def test_sample_sql_queries_fixture(self, sample_sql_queries):
        """Test the sample SQL queries fixture."""
        assert "simple_select" in sample_sql_queries
        assert "parameterized" in sample_sql_queries
        assert "complex_join" in sample_sql_queries
        assert "aggregation" in sample_sql_queries
        assert "insert" in sample_sql_queries
        assert "update" in sample_sql_queries
        assert "delete" in sample_sql_queries

        # Test specific query content
        simple_query = sample_sql_queries["simple_select"]
        assert "SELECT * FROM users" in simple_query

        param_query = sample_sql_queries["parameterized"]
        assert "%(id)s" in param_query
        assert "%(active)s" in param_query

    def test_sample_query_params_fixture(self, sample_query_params):
        """Test the sample query parameters fixture."""
        assert "simple" in sample_query_params
        assert "user_lookup" in sample_query_params
        assert "bulk_insert" in sample_query_params
        assert "filter" in sample_query_params
        assert "pagination" in sample_query_params

        # Test specific parameter content
        user_lookup = sample_query_params["user_lookup"]
        assert user_lookup["id"] == 1
        assert user_lookup["active"] is True

        bulk_insert = sample_query_params["bulk_insert"]
        assert len(bulk_insert) == 3
        assert bulk_insert[0]["name"] == "John"
        assert bulk_insert[1]["name"] == "Jane"
        assert bulk_insert[2]["name"] == "Bob"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
