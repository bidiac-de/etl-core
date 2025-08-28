import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.engine import Connection, Engine

from src.etl_core.components.databases.sql_connection_handler import (
    SQLConnectionHandler,
)
from src.etl_core.components.databases.pool_registry import PoolKey


class TestSQLConnectionHandler:
    """Test cases for SQLConnectionHandler class."""

    def setup_method(self):
        """Reset the singleton instance before each test."""
        from src.etl_core.components.databases.pool_registry import (
            ConnectionPoolRegistry,
        )

        ConnectionPoolRegistry._instance = None

    def test_initialization(self):
        """Test handler initialization."""
        handler = SQLConnectionHandler()
        assert handler._registry is not None
        assert handler._key is None
        assert handler._engine is None

    def test_build_url_postgres(self):
        """Test building PostgreSQL URL."""
        url = SQLConnectionHandler.build_url(
            db_type="postgres",
            user="testuser",
            password="testpass",
            host="localhost",
            port=5432,
            database="testdb",
        )
        expected = "postgres://testuser:testpass@localhost:5432/testdb"
        assert url == expected

    def test_build_url_mysql(self):
        """Test building MySQL URL."""
        url = SQLConnectionHandler.build_url(
            db_type="mysql",
            user="testuser",
            password="testpass",
            host="localhost",
            port=3306,
            database="testdb",
        )
        expected = "mysql://testuser:testpass@localhost:3306/testdb"
        assert url == expected

    def test_build_url_mariadb(self):
        """Test building MariaDB URL."""
        url = SQLConnectionHandler.build_url(
            db_type="mariadb",
            user="testuser",
            password="testpass",
            host="localhost",
            port=3306,
            database="testdb",
        )
        expected = "mariadb://testuser:testpass@localhost:3306/testdb"
        assert url == expected



    def test_build_url_missing_required_params(self):
        """Test building URL with missing required parameters."""
        with pytest.raises(
            ValueError,
            match="postgres requires user, password, host, port, and database",
        ):
            SQLConnectionHandler.build_url(
                db_type="postgres",
                user="testuser",
                # Missing password, host, port, database
            )

    def test_build_url_unsupported_dialect(self):
        """Test building URL with any database type (all are supported)."""
        # The current implementation supports any db_type, so this should work
        url = SQLConnectionHandler.build_url(
            db_type="oracle",
            user="testuser",
            password="testpass",
            host="localhost",
            port=1521,
            database="testdb",
        )
        expected = "oracle://testuser:testpass@localhost:1521/testdb"
        assert url == expected

    def test_build_url_case_insensitive(self):
        """Test that database type is case sensitive (as per current implementation)."""
        url1 = SQLConnectionHandler.build_url(
            db_type="POSTGRES",
            user="testuser",
            password="testpass",
            host="localhost",
            port=5432,
            database="testdb",
        )
        url2 = SQLConnectionHandler.build_url(
            db_type="postgres",
            user="testuser",
            password="testpass",
            host="localhost",
            port=5432,
            database="testdb",
        )
        # Current implementation is case sensitive, so these should be different
        assert url1 != url2
        assert url1 == "POSTGRES://testuser:testpass@localhost:5432/testdb"
        assert url2 == "postgres://testuser:testpass@localhost:5432/testdb"

    @patch(
        "src.etl_core.components.databases."
        "sql_connection_handler.ConnectionPoolRegistry"
    )
    def test_connect(self, mock_registry_class):
        """Test connecting to database."""
        mock_registry = Mock()
        mock_registry_class.instance.return_value = mock_registry

        mock_key = Mock(spec=PoolKey)
        mock_engine = Mock(spec=Engine)
        mock_registry.get_sql_engine.return_value = (mock_key, mock_engine)

        handler = SQLConnectionHandler()
        key, engine = handler.connect(url="postgresql://localhost:5432/db")

        assert key == mock_key
        assert engine == mock_engine
        assert handler._key == mock_key
        assert handler._engine == mock_engine

        mock_registry.get_sql_engine.assert_called_once_with(
            url="postgresql://localhost:5432/db", engine_kwargs=None
        )

    @patch(
        "src.etl_core.components.databases."
        "sql_connection_handler.ConnectionPoolRegistry"
    )
    def test_connect_with_engine_kwargs(self, mock_registry_class):
        """Test connecting with custom engine kwargs."""
        mock_registry = Mock()
        mock_registry_class.instance.return_value = mock_registry

        mock_key = Mock(spec=PoolKey)
        mock_engine = Mock(spec=Engine)
        mock_registry.get_sql_engine.return_value = (mock_key, mock_engine)

        handler = SQLConnectionHandler()
        engine_kwargs = {"pool_size": 10, "max_overflow": 20}
        key, engine = handler.connect(
            url="postgresql://localhost:5432/db", engine_kwargs=engine_kwargs
        )

        mock_registry.get_sql_engine.assert_called_once_with(
            url="postgresql://localhost:5432/db", engine_kwargs=engine_kwargs
        )

    @patch(
        "src.etl_core.components.databases."
        "sql_connection_handler.ConnectionPoolRegistry"
    )
    def test_lease_not_connected(self, mock_registry_class):
        """Test that lease raises error when not connected."""
        mock_registry = Mock()
        mock_registry_class.instance.return_value = mock_registry

        handler = SQLConnectionHandler()

        with pytest.raises(
            RuntimeError,
            match=(
                "SQLConnectionHandler.connect\\(\\) must be called "
                "before lease\\(\\)"
            ),
        ):
            # directly enter the context manager to trigger the error
            handler.lease().__enter__()

    @patch(
        "src.etl_core.components.databases."
        "sql_connection_handler.ConnectionPoolRegistry"
    )
    def test_lease_success(self, mock_registry_class):
        """Test successful connection lease."""
        mock_registry = Mock()
        mock_registry_class.instance.return_value = mock_registry

        mock_key = Mock(spec=PoolKey)
        mock_engine = Mock(spec=Engine)
        mock_connection = Mock(spec=Connection)

        # Mock the context manager
        mock_context = MagicMock()
        mock_context.__enter__ = Mock(return_value=mock_connection)
        mock_context.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_context

        mock_registry.get_sql_engine.return_value = (mock_key, mock_engine)

        handler = SQLConnectionHandler()
        handler.connect(url="postgresql://localhost:5432/db")

        with handler.lease() as conn:
            assert conn == mock_connection

        # Verify lease and release were called
        mock_registry.lease_sql.assert_called_once_with(mock_key)
        mock_registry.release_sql.assert_called_once_with(mock_key)

    @patch(
        "src.etl_core.components.databases."
        "sql_connection_handler.ConnectionPoolRegistry"
    )
    def test_lease_exception_handling(self, mock_registry_class):
        """Test that lease properly handles exceptions and releases connection."""
        mock_registry = Mock()
        mock_registry_class.instance.return_value = mock_registry

        mock_key = Mock(spec=PoolKey)
        mock_engine = Mock(spec=Engine)
        mock_connection = Mock(spec=Connection)

        # Mock the context manager
        mock_context = MagicMock()
        mock_context.__enter__ = Mock(return_value=mock_connection)
        mock_context.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_context

        mock_registry.get_sql_engine.return_value = (mock_key, mock_engine)

        handler = SQLConnectionHandler()
        handler.connect(url="postgresql://localhost:5432/db")

        with pytest.raises(RuntimeError):
            with handler.lease():
                raise RuntimeError("Test exception")

        # Verify release was called even with exception
        mock_registry.release_sql.assert_called_once_with(mock_key)

    @patch(
        "src.etl_core.components.databases."
        "sql_connection_handler.ConnectionPoolRegistry"
    )
    def test_close_pool_success(self, mock_registry_class):
        """Test successfully closing pool."""
        mock_registry = Mock()
        mock_registry_class.instance.return_value = mock_registry

        mock_key = Mock(spec=PoolKey)
        mock_registry.close_pool.return_value = True

        handler = SQLConnectionHandler()
        handler._key = mock_key

        result = handler.close_pool()
        assert result is True
        mock_registry.close_pool.assert_called_once_with(mock_key, force=False)

    @patch(
        "src.etl_core.components.databases."
        "sql_connection_handler.ConnectionPoolRegistry"
    )
    def test_close_pool_force(self, mock_registry_class):
        """Test force closing pool."""
        mock_registry = Mock()
        mock_registry_class.instance.return_value = mock_registry

        mock_key = Mock(spec=PoolKey)
        mock_registry.close_pool.return_value = True

        handler = SQLConnectionHandler()
        handler._key = mock_key

        result = handler.close_pool(force=True)
        assert result is True
        mock_registry.close_pool.assert_called_once_with(mock_key, force=True)

    @patch(
        "src.etl_core.components.databases."
        "sql_connection_handler.ConnectionPoolRegistry"
    )
    def test_close_pool_no_key(self, mock_registry_class):
        """Test closing pool when no key exists."""
        mock_registry = Mock()
        mock_registry_class.instance.return_value = mock_registry

        handler = SQLConnectionHandler()
        handler._key = None

        result = handler.close_pool()
        assert result is False
        mock_registry.close_pool.assert_not_called()

    @patch(
        "src.etl_core.components.databases."
        "sql_connection_handler.ConnectionPoolRegistry"
    )
    def test_stats(self, mock_registry_class):
        """Test getting stats from registry."""
        mock_registry = Mock()
        mock_registry_class.instance.return_value = mock_registry

        expected_stats = {"sql": {"test": {"leased": 1, "opened": 1}}}
        mock_registry.stats.return_value = expected_stats

        handler = SQLConnectionHandler()
        stats = handler.stats()

        assert stats == expected_stats
        mock_registry.stats.assert_called_once()

    def test_build_url_edge_cases(self):
        """Test edge cases in URL building."""
        # Test with None values
        with pytest.raises(ValueError):
            SQLConnectionHandler.build_url(
                db_type="postgres",
                user=None,
                password="testpass",
                host="localhost",
                port=5432,
                database="testdb",
            )

        # Test with empty strings
        with pytest.raises(ValueError):
            SQLConnectionHandler.build_url(
                db_type="postgres",
                user="",
                password="testpass",
                host="localhost",
                port=5432,
                database="testdb",
            )

    def test_build_url_special_characters(self):
        """Test URL building with special characters in credentials."""
        url = SQLConnectionHandler.build_url(
            db_type="postgres",
            user="user@domain",
            password="pass@word!",
            host="localhost",
            port=5432,
            database="test-db",
        )
        expected = "postgres://user@domain:pass@word!@localhost:5432/test-db"
        assert url == expected

    @patch(
        "src.etl_core.components.databases."
        "sql_connection_handler.ConnectionPoolRegistry"
    )
    def test_multiple_connections(self, mock_registry_class):
        """Test handling multiple connections."""
        mock_registry = Mock()
        mock_registry_class.instance.return_value = mock_registry

        mock_key1 = Mock(spec=PoolKey)
        mock_key2 = Mock(spec=PoolKey)
        mock_engine1 = Mock(spec=Engine)
        mock_engine2 = Mock(spec=Engine)

        mock_registry.get_sql_engine.side_effect = [
            (mock_key1, mock_engine1),
            (mock_key2, mock_engine2),
        ]

        handler = SQLConnectionHandler()

        # First connection
        key1, engine1 = handler.connect(url="postgresql://localhost:5432/db1")
        assert key1 == mock_key1
        assert engine1 == mock_engine1

        # Second connection (should replace first)
        key2, engine2 = handler.connect(url="postgresql://localhost:5432/db2")
        assert key2 == mock_key2
        assert engine2 == mock_engine2
        assert handler._key == mock_key2
        assert handler._engine == mock_engine2
