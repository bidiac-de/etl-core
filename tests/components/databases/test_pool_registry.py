import pytest
from unittest.mock import Mock, patch
from sqlalchemy.engine import Engine
from pymongo import MongoClient

from src.etl_core.components.databases.pool_registry import (
    ConnectionPoolRegistry,
    PoolKey,
)


class TestPoolKey:
    """Test cases for PoolKey class."""

    def test_pool_key_creation(self):
        """Test basic PoolKey creation."""
        key = PoolKey(kind="sql", dsn="test://localhost:5432/db")
        assert key.kind == "sql"
        assert key.dsn == "test://localhost:5432/db"

    def test_pool_key_immutability(self):
        """Test that PoolKey is immutable."""
        key = PoolKey(kind="sql", dsn="test://localhost:5432/db")
        with pytest.raises(Exception):
            key.kind = "mongo"

    def test_pool_key_equality(self):
        """Test PoolKey equality comparison."""
        key1 = PoolKey(kind="sql", dsn="test://localhost:5432/db")
        key2 = PoolKey(kind="sql", dsn="test://localhost:5432/db")
        key3 = PoolKey(kind="mongo", dsn="test://localhost:5432/db")

        assert key1 == key2
        assert key1 != key3
        assert hash(key1) == hash(key2)

    def test_normalize_pairs(self):
        """Test _normalize_pairs static method."""
        pairs = {"b": 2, "a": 1, "c": 3}
        normalized = PoolKey._normalize_pairs(pairs)
        expected = (("a", 1), ("b", 2), ("c", 3))
        assert normalized == expected

    def test_for_sql_without_kwargs(self):
        """Test for_sql class method without engine kwargs."""
        key = PoolKey.for_sql(url="postgresql://localhost:5432/db")
        assert key.kind == "sql"
        assert "postgresql://localhost:5432/db##" in key.dsn

    def test_for_sql_with_kwargs(self):
        """Test for_sql class method with engine kwargs."""
        engine_kwargs = {"pool_size": 10, "max_overflow": 20}
        key1 = PoolKey.for_sql(
            url="postgresql://localhost:5432/db", engine_kwargs=engine_kwargs
        )
        key2 = PoolKey.for_sql(
            url="postgresql://localhost:5432/db", engine_kwargs=engine_kwargs
        )

        assert key1 == key2
        assert key1.kind == "sql"

    def test_for_sql_different_kwargs(self):
        """Test that different engine kwargs create different keys."""
        key1 = PoolKey.for_sql(
            url="postgresql://localhost:5432/db", engine_kwargs={"pool_size": 10}
        )
        key2 = PoolKey.for_sql(
            url="postgresql://localhost:5432/db", engine_kwargs={"pool_size": 20}
        )


        assert key1 != key2

    def test_for_mongo_without_kwargs(self):
        """Test for_mongo class method without client kwargs."""
        key = PoolKey.for_mongo(uri="mongodb://localhost:27017")
        assert key.kind == "mongo"
        assert "mongodb://localhost:27017##" in key.dsn

    def test_for_mongo_with_kwargs(self):
        """Test for_mongo class method with client kwargs."""
        client_kwargs = {"maxPoolSize": 10, "serverSelectionTimeoutMS": 5000}
        key1 = PoolKey.for_mongo(
            uri="mongodb://localhost:27017", client_kwargs=client_kwargs
        )
        key2 = PoolKey.for_mongo(
            uri="mongodb://localhost:27017", client_kwargs=client_kwargs
        )

        assert key1 == key2
        assert key1.kind == "mongo"

    def test_for_mongo_different_kwargs(self):
        """Test that different client kwargs create different keys."""

        key1 = PoolKey.for_mongo(
            uri="mongodb://localhost:27017", client_kwargs={"maxPoolSize": 10}
        )
        key2 = PoolKey.for_mongo(
            uri="mongodb://localhost:27017", client_kwargs={"maxPoolSize": 20}
        )

        assert key1 != key2


class TestConnectionPoolRegistry:
    """Test cases for ConnectionPoolRegistry class."""

    def setup_method(self):
        """Reset the singleton instance before each test."""
        ConnectionPoolRegistry._instance = None

    def test_singleton_pattern(self):
        """Test that ConnectionPoolRegistry follows singleton pattern."""
        registry1 = ConnectionPoolRegistry.instance()
        registry2 = ConnectionPoolRegistry.instance()
        assert registry1 is registry2

    def test_initialization(self):
        """Test registry initialization."""
        registry = ConnectionPoolRegistry()
        assert registry._sql == {}
        assert registry._mongo == {}

    @patch("src.etl_core.components.databases.pool_registry.create_engine")
    
    def test_get_sql_engine_new_connection(self, mock_create_engine):
        """Test getting a new SQL engine."""
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        registry = ConnectionPoolRegistry()
        key, engine = registry.get_sql_engine(url="postgresql://localhost:5432/db")

        assert isinstance(key, PoolKey)
        assert key.kind == "sql"
        assert engine == mock_engine
        mock_create_engine.assert_called_once_with(
            "postgresql://localhost:5432/db", **{}
        )

    @patch("src.etl_core.components.databases.pool_registry.create_engine")

    def test_get_sql_engine_existing_connection(self, mock_create_engine):
        """Test getting an existing SQL engine."""
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        registry = ConnectionPoolRegistry()
        key1, engine1 = registry.get_sql_engine(url="postgresql://localhost:5432/db")
        key2, engine2 = registry.get_sql_engine(url="postgresql://localhost:5432/db")

        assert key1 == key2
        assert engine1 is engine2
        # create_engine should only be called once
        mock_create_engine.assert_called_once()

    @patch("src.etl_core.components.databases.pool_registry.create_engine")

    def test_get_sql_engine_with_kwargs(self, mock_create_engine):
        """Test getting SQL engine with custom kwargs."""
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        registry = ConnectionPoolRegistry()
        engine_kwargs = {"pool_size": 10, "max_overflow": 20}
        key, engine = registry.get_sql_engine(
            url="postgresql://localhost:5432/db", engine_kwargs=engine_kwargs
        )

        mock_create_engine.assert_called_once_with(
            "postgresql://localhost:5432/db", **engine_kwargs
        )


    def test_lease_sql(self):
        """Test leasing SQL connection."""
        registry = ConnectionPoolRegistry()
        key, _ = registry.get_sql_engine(url="postgresql://localhost:5432/db")

        registry.lease_sql(key)
        stats = registry.stats()

        assert stats["sql"][key.dsn]["leased"] == 1
        assert stats["sql"][key.dsn]["opened"] == 1

    def test_release_sql(self):
        """Test releasing SQL connection."""
        registry = ConnectionPoolRegistry()
        key, _ = registry.get_sql_engine(url="postgresql://localhost:5432/db")

        registry.lease_sql(key)
        registry.lease_sql(key)
        registry.release_sql(key)

        stats = registry.stats()
        assert stats["sql"][key.dsn]["leased"] == 1
        assert stats["sql"][key.dsn]["opened"] == 2

    def test_release_sql_nonexistent(self):
        """Test releasing non-existent SQL connection."""
        registry = ConnectionPoolRegistry()
        key = PoolKey(kind="sql", dsn="nonexistent")

        # Should not raise an error
        registry.release_sql(key)

    @patch("src.etl_core.components.databases.pool_registry.MongoClient")

    def test_get_mongo_client_new_connection(self, mock_mongo_client):
        """Test getting a new MongoDB client."""
        mock_client = Mock(spec=MongoClient)
        mock_mongo_client.return_value = mock_client

        registry = ConnectionPoolRegistry()
        key, client = registry.get_mongo_client(uri="mongodb://localhost:27017")

        assert isinstance(key, PoolKey)
        assert key.kind == "mongo"
        assert client == mock_client
        mock_mongo_client.assert_called_once_with("mongodb://localhost:27017", **{})


    @patch("src.etl_core.components.databases.pool_registry.MongoClient")
    
    def test_get_mongo_client_existing_connection(self, mock_mongo_client):
        """Test getting an existing MongoDB client."""
        mock_client = Mock(spec=MongoClient)
        mock_mongo_client.return_value = mock_client

        registry = ConnectionPoolRegistry()
        key1, client1 = registry.get_mongo_client(uri="mongodb://localhost:27017")
        key2, client2 = registry.get_mongo_client(uri="mongodb://localhost:27017")

        assert key1 == key2
        assert client1 is client2
        # MongoClient should only be called once
        mock_mongo_client.assert_called_once()

    def test_lease_mongo(self):
        """Test leasing MongoDB connection."""
        registry = ConnectionPoolRegistry()
        key, _ = registry.get_mongo_client(uri="mongodb://localhost:27017")

        registry.lease_mongo(key)
        stats = registry.stats()

        assert stats["mongo"][key.dsn]["leased"] == 1
        assert stats["mongo"][key.dsn]["opened"] == 1

    def test_release_mongo(self):
        """Test releasing MongoDB connection."""
        registry = ConnectionPoolRegistry()
        key, _ = registry.get_mongo_client(uri="mongodb://localhost:27017")

        registry.lease_mongo(key)
        registry.lease_mongo(key)
        registry.release_mongo(key)

        stats = registry.stats()
        assert stats["mongo"][key.dsn]["leased"] == 1
        assert stats["mongo"][key.dsn]["opened"] == 2

    def test_stats_empty(self):
        """Test stats when no connections exist."""
        registry = ConnectionPoolRegistry()
        stats = registry.stats()


        assert stats == {"sql": {}, "mongo": {}}

    def test_stats_with_connections(self):
        """Test stats with active connections."""
        registry = ConnectionPoolRegistry()

        # Create some connections
        sql_key, _ = registry.get_sql_engine(url="postgresql://localhost:5432/db")
        mongo_key, _ = registry.get_mongo_client(uri="mongodb://localhost:27017")

        # Lease them
        registry.lease_sql(sql_key)
        registry.lease_mongo(mongo_key)

        stats = registry.stats()

        assert "sql" in stats
        assert "mongo" in stats
        assert sql_key.dsn in stats["sql"]
        assert mongo_key.dsn in stats["mongo"]

    def test_close_pool_sql_success(self):
        """Test successfully closing SQL pool."""
        registry = ConnectionPoolRegistry()
        key, engine = registry.get_sql_engine(url="postgresql://localhost:5432/db")

        # Mock the dispose method
        engine.dispose = Mock()

        result = registry.close_pool(key)
        assert result is True
        engine.dispose.assert_called_once()

    def test_close_pool_sql_with_leases(self):
        """Test closing SQL pool with active leases."""
        registry = ConnectionPoolRegistry()
        key, _ = registry.get_sql_engine(url="postgresql://localhost:5432/db")

        registry.lease_sql(key)
        result = registry.close_pool(key)

        # Should fail because there are active leases
        assert result is False

    def test_close_pool_sql_force_close(self):
        """Test force closing SQL pool with active leases."""
        registry = ConnectionPoolRegistry()
        key, engine = registry.get_sql_engine(url="postgresql://localhost:5432/db")

        registry.lease_sql(key)
        engine.dispose = Mock()

        result = registry.close_pool(key, force=True)
        assert result is True
        engine.dispose.assert_called_once()

    def test_close_pool_mongo_success(self):
        """Test successfully closing MongoDB pool."""
        registry = ConnectionPoolRegistry()
        key, client = registry.get_mongo_client(uri="mongodb://localhost:27017")

        # Mock the close method
        client.close = Mock()

        result = registry.close_pool(key)
        assert result is True
        client.close.assert_called_once()

    def test_close_pool_nonexistent(self):
        """Test closing non-existent pool."""
        registry = ConnectionPoolRegistry()
        key = PoolKey(kind="sql", dsn="nonexistent")

        result = registry.close_pool(key)
        assert result is False

    def test_close_pool_invalid_kind(self):
        """Test closing pool with invalid kind."""
        registry = ConnectionPoolRegistry()
        key = PoolKey(kind="invalid", dsn="test")

        result = registry.close_pool(key)
        assert result is False

    def test_thread_safety(self):
        """Test that registry operations are thread-safe."""
        import threading
        import time

        registry = ConnectionPoolRegistry()
        results = []

        def worker():
            try:
                key, _ = registry.get_sql_engine(url="postgresql://localhost:5432/db")
                registry.lease_sql(key)
                time.sleep(0.01)  # Simulate work
                registry.release_sql(key)
                results.append(True)
            except Exception:
                results.append(False)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()


        # All operations should succeed without race conditions
        assert all(results)
        assert len(results) == 10
