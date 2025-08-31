import threading
import time

import pytest
from sqlalchemy.engine import Engine
from motor.motor_asyncio import AsyncIOMotorClient

from src.etl_core.components.databases.pool_registry import (
    ConnectionPoolRegistry,
    PoolKey,
)


class TestPoolKey:
    """Test cases for PoolKey class."""

    def test_pool_key_creation(self) -> None:
        key = PoolKey(kind="sql", dsn="test://localhost:5432/db")
        assert key.kind == "sql"
        assert key.dsn == "test://localhost:5432/db"

    def test_pool_key_immutability(self) -> None:
        key = PoolKey(kind="sql", dsn="test://localhost:5432/db")
        with pytest.raises(Exception):
            # dataclass(frozen=True) -> attributes are read-only
            key.kind = "mongo"  # type: ignore[attr-defined]

    def test_pool_key_equality(self) -> None:
        key1 = PoolKey(kind="sql", dsn="test://localhost:5432/db")
        key2 = PoolKey(kind="sql", dsn="test://localhost:5432/db")
        key3 = PoolKey(kind="mongo", dsn="test://localhost:5432/db")
        assert key1 == key2
        assert key1 != key3
        assert hash(key1) == hash(key2)

    def test_normalize_pairs(self) -> None:
        pairs = {"b": 2, "a": 1, "c": 3}
        normalized = PoolKey._normalize_pairs(pairs)
        expected = (("a", 1), ("b", 2), ("c", 3))
        assert normalized == expected

    def test_for_sql_without_kwargs(self) -> None:
        key = PoolKey.for_sql(url="sqlite:///:memory:")
        assert key.kind == "sql"
        assert "sqlite:///:memory:##" in key.dsn

    def test_for_sql_with_kwargs(self) -> None:
        engine_kwargs = {"pool_size": 10, "max_overflow": 20}
        key1 = PoolKey.for_sql(url="sqlite:///:memory:", engine_kwargs=engine_kwargs)
        key2 = PoolKey.for_sql(url="sqlite:///:memory:", engine_kwargs=engine_kwargs)
        assert key1 == key2
        assert key1.kind == "sql"

    def test_for_sql_different_kwargs(self) -> None:
        key1 = PoolKey.for_sql(url="sqlite:///:memory:", engine_kwargs={"pool_size": 5})
        key2 = PoolKey.for_sql(url="sqlite:///:memory:", engine_kwargs={"pool_size": 10})
        assert key1 != key2

    def test_for_mongo_without_kwargs(self) -> None:
        key = PoolKey.for_mongo(uri="mongodb://localhost:27017")
        assert key.kind == "mongo"
        assert "mongodb://localhost:27017##" in key.dsn

    def test_for_mongo_with_kwargs(self) -> None:
        client_kwargs = {"maxPoolSize": 10, "serverSelectionTimeoutMS": 5000}
        key1 = PoolKey.for_mongo(uri="mongodb://localhost:27017", client_kwargs=client_kwargs)
        key2 = PoolKey.for_mongo(uri="mongodb://localhost:27017", client_kwargs=client_kwargs)
        assert key1 == key2
        assert key1.kind == "mongo"

    def test_for_mongo_different_kwargs(self) -> None:
        key1 = PoolKey.for_mongo(uri="mongodb://localhost:27017", client_kwargs={"maxPoolSize": 10})
        key2 = PoolKey.for_mongo(uri="mongodb://localhost:27017", client_kwargs={"maxPoolSize": 20})
        assert key1 != key2


class TestConnectionPoolRegistry:
    """Test cases for ConnectionPoolRegistry class."""

    def setup_method(self) -> None:
        # Reset the singleton instance before each test.
        ConnectionPoolRegistry._instance = None  # type: ignore[attr-defined]

    def test_singleton_pattern(self) -> None:
        registry1 = ConnectionPoolRegistry.instance()
        registry2 = ConnectionPoolRegistry.instance()
        assert registry1 is registry2

    def test_initialization(self) -> None:
        registry = ConnectionPoolRegistry()
        assert registry._sql == {}  # type: ignore[attr-defined]
        assert registry._mongo == {}  # type: ignore[attr-defined]

    # ---------- SQL tests (real engine, in-memory SQLite) ----------

    def test_get_sql_engine_new_connection(self) -> None:
        registry = ConnectionPoolRegistry()
        key, engine = registry.get_sql_engine(url="sqlite:///:memory:")
        assert isinstance(key, PoolKey)
        assert key.kind == "sql"
        assert isinstance(engine, Engine)
        # str(engine.url) is observable behavior of created engine
        assert str(engine.url).startswith("sqlite://")

    def test_get_sql_engine_existing_connection(self) -> None:
        registry = ConnectionPoolRegistry()
        key1, engine1 = registry.get_sql_engine(url="sqlite:///:memory:")
        key2, engine2 = registry.get_sql_engine(url="sqlite:///:memory:")
        assert key1 == key2
        assert engine1 is engine2  # pooled

    def test_get_sql_engine_with_kwargs_distinguishes_pools(self) -> None:
        registry = ConnectionPoolRegistry()
        kwargs_a = {"pool_size": 5}
        kwargs_b = {"pool_size": 10}
        key_a, engine_a = registry.get_sql_engine(url="sqlite:///:memory:", engine_kwargs=kwargs_a)
        key_b, engine_b = registry.get_sql_engine(url="sqlite:///:memory:", engine_kwargs=kwargs_b)
        assert key_a != key_b
        assert engine_a is not engine_b

    def test_lease_sql(self) -> None:
        registry = ConnectionPoolRegistry()
        key, _ = registry.get_sql_engine(url="sqlite:///:memory:")
        registry.lease_sql(key)
        stats = registry.stats()
        assert stats["sql"][key.dsn]["leased"] == 1
        assert stats["sql"][key.dsn]["opened"] == 1

    def test_release_sql(self) -> None:
        registry = ConnectionPoolRegistry()
        key, _ = registry.get_sql_engine(url="sqlite:///:memory:")
        registry.lease_sql(key)
        registry.lease_sql(key)
        registry.release_sql(key)
        stats = registry.stats()
        assert stats["sql"][key.dsn]["leased"] == 1
        assert stats["sql"][key.dsn]["opened"] == 2

    def test_release_sql_nonexistent(self) -> None:
        registry = ConnectionPoolRegistry()
        key = PoolKey(kind="sql", dsn="nonexistent")
        # should not raise
        registry.release_sql(key)

    def test_close_pool_sql_success(self) -> None:
        registry = ConnectionPoolRegistry()
        key, engine = registry.get_sql_engine(url="sqlite:///:memory:")
        # close without active leases
        result = registry.close_pool(key)
        assert result is True
        # engine has been disposed and removed from stats
        stats = registry.stats()
        assert key.dsn not in stats["sql"]
        # engine.dispose() is idempotent; calling again shouldn't be necessary,
        # but ensure the object still has the attribute
        assert hasattr(engine, "dispose")

    def test_close_pool_sql_with_leases(self) -> None:
        registry = ConnectionPoolRegistry()
        key, _ = registry.get_sql_engine(url="sqlite:///:memory:")
        registry.lease_sql(key)
        result = registry.close_pool(key)
        assert result is False  # active leases prevent close

    def test_close_pool_sql_force_close(self) -> None:
        registry = ConnectionPoolRegistry()
        key, _ = registry.get_sql_engine(url="sqlite:///:memory:")
        registry.lease_sql(key)
        result = registry.close_pool(key, force=True)
        assert result is True
        stats = registry.stats()
        assert key.dsn not in stats["sql"]

    # ---------- Mongo tests (real AsyncIOMotorClient, no patching) ----------

    def test_get_mongo_client_new_connection(self) -> None:
        registry = ConnectionPoolRegistry()
        key, client = registry.get_mongo_client(uri="mongodb://localhost:27017")
        assert isinstance(key, PoolKey)
        assert key.kind == "mongo"
        assert isinstance(client, AsyncIOMotorClient)

    def test_get_mongo_client_existing_connection(self) -> None:
        registry = ConnectionPoolRegistry()
        key1, client1 = registry.get_mongo_client(uri="mongodb://localhost:27017")
        key2, client2 = registry.get_mongo_client(uri="mongodb://localhost:27017")
        assert key1 == key2
        assert client1 is client2  # pooled

    def test_lease_mongo(self) -> None:
        registry = ConnectionPoolRegistry()
        key, _ = registry.get_mongo_client(uri="mongodb://localhost:27017")
        registry.lease_mongo(key)
        stats = registry.stats()
        assert stats["mongo"][key.dsn]["leased"] == 1
        assert stats["mongo"][key.dsn]["opened"] == 1

    def test_release_mongo(self) -> None:
        registry = ConnectionPoolRegistry()
        key, _ = registry.get_mongo_client(uri="mongodb://localhost:27017")
        registry.lease_mongo(key)
        registry.lease_mongo(key)
        registry.release_mongo(key)
        stats = registry.stats()
        assert stats["mongo"][key.dsn]["leased"] == 1
        assert stats["mongo"][key.dsn]["opened"] == 2

    def test_stats_empty(self) -> None:
        registry = ConnectionPoolRegistry()
        assert registry.stats() == {"sql": {}, "mongo": {}}

    def test_stats_with_connections(self) -> None:
        registry = ConnectionPoolRegistry()
        sql_key, _ = registry.get_sql_engine(url="sqlite:///:memory:")
        mongo_key, _ = registry.get_mongo_client(uri="mongodb://localhost:27017")
        registry.lease_sql(sql_key)
        registry.lease_mongo(mongo_key)
        stats = registry.stats()
        assert "sql" in stats and "mongo" in stats
        assert sql_key.dsn in stats["sql"]
        assert mongo_key.dsn in stats["mongo"]

    def test_close_pool_mongo_success(self) -> None:
        registry = ConnectionPoolRegistry()
        key, client = registry.get_mongo_client(uri="mongodb://localhost:27017")
        # Close without active leases
        result = registry.close_pool(key)
        assert result is True
        # removed from stats
        stats = registry.stats()
        assert key.dsn not in stats["mongo"]
        # client exposes .close()
        assert hasattr(client, "close")

    def test_close_pool_nonexistent(self) -> None:
        registry = ConnectionPoolRegistry()
        key = PoolKey(kind="sql", dsn="nonexistent")
        assert registry.close_pool(key) is False

    def test_close_pool_invalid_kind(self) -> None:
        registry = ConnectionPoolRegistry()
        key = PoolKey(kind="invalid", dsn="test")
        assert registry.close_pool(key) is False

    def test_thread_safety(self) -> None:
        registry = ConnectionPoolRegistry()
        results = []

        def worker() -> None:
            try:
                key, _ = registry.get_sql_engine(url="sqlite:///:memory:")
                registry.lease_sql(key)
                time.sleep(0.01)
                registry.release_sql(key)
                results.append(True)
            except Exception:
                results.append(False)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(results)
        assert len(results) == 10
