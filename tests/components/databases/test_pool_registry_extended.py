from __future__ import annotations

import time
from typing import List

import pytest

from etl_core.components.databases.pool_registry import ConnectionPoolRegistry


class _FakeTimer:
    """Minimal stand-in for threading.Timer that doesn't spawn threads."""

    created: List["_FakeTimer"] = []

    def __init__(self, delay: float, func, args=(), kwargs=None) -> None:  # noqa: D401
        self.delay = delay
        self.func = func
        self.args = args
        self.kwargs = kwargs or {}
        self.started = False
        self.canceled = False
        _FakeTimer.created.append(self)

    def start(self) -> None:
        self.started = True

    def cancel(self) -> None:
        self.canceled = True


class _FakeClient:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _reset_singleton() -> None:
    ConnectionPoolRegistry._instance = None  # type: ignore[attr-defined]


def test__load_idle_timeout_variants(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ETL_POOL_IDLE_TIMEOUT_SECONDS", raising=False)
    r1 = ConnectionPoolRegistry()
    assert r1._idle_timeout_seconds == 300  # type: ignore[attr-defined]

    monkeypatch.setenv("ETL_POOL_IDLE_TIMEOUT_SECONDS", "42")
    r2 = ConnectionPoolRegistry()
    assert r2._idle_timeout_seconds == 42  # type: ignore[attr-defined]

    monkeypatch.setenv("ETL_POOL_IDLE_TIMEOUT_SECONDS", "0")
    r3 = ConnectionPoolRegistry()
    assert r3._idle_timeout_seconds == 0  # type: ignore[attr-defined]

    monkeypatch.setenv("ETL_POOL_IDLE_TIMEOUT_SECONDS", "abc")
    r4 = ConnectionPoolRegistry()
    assert r4._idle_timeout_seconds == 300  # type: ignore[attr-defined]

    monkeypatch.setenv("ETL_POOL_IDLE_TIMEOUT_SECONDS", "-5")
    r5 = ConnectionPoolRegistry()
    assert r5._idle_timeout_seconds == 300  # type: ignore[attr-defined]


def test_sql_timer_scheduled_and_canceled_on_reuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "etl_core.components.databases.pool_registry.threading.Timer", _FakeTimer
    )

    monkeypatch.setenv("ETL_POOL_IDLE_TIMEOUT_SECONDS", "10")
    reg = ConnectionPoolRegistry()
    key, _ = reg.get_sql_engine(url="sqlite:///:memory:")

    reg.lease_sql(key)
    reg.release_sql(key)

    assert _FakeTimer.created, "Expected idle close timer to be created for SQL"
    t = _FakeTimer.created[-1]
    assert t.started is True and t.canceled is False

    reg.get_sql_engine(url="sqlite:///:memory:")
    assert t.canceled is True


def test_mongo_timer_scheduled_and_canceled_on_reuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "etl_core.components.databases.pool_registry.threading.Timer", _FakeTimer
    )

    monkeypatch.setenv("ETL_POOL_IDLE_TIMEOUT_SECONDS", "10")
    reg = ConnectionPoolRegistry()
    key, _ = reg.get_mongo_client(uri="mongodb://localhost:27017")

    reg.lease_mongo(key)
    reg.release_mongo(key)

    assert _FakeTimer.created, "Expected idle close timer to be created for Mongo"
    t = _FakeTimer.created[-1]
    assert t.started is True and t.canceled is False

    reg.get_mongo_client(uri="mongodb://localhost:27017")
    assert t.canceled is True


def test_idle_auto_close_with_zero_timeout_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ETL_POOL_IDLE_TIMEOUT_SECONDS", "0")
    reg = ConnectionPoolRegistry()
    key, _ = reg.get_sql_engine(url="sqlite:///:memory:")

    reg.lease_sql(key)
    reg.release_sql(key)

    time.sleep(0.03)
    stats = reg.stats()
    assert key.dsn not in stats["sql"]


def test_register_mongo_client_replaces_and_closes_previous() -> None:
    reg = ConnectionPoolRegistry()
    c1 = _FakeClient()
    c2 = _FakeClient()

    key1 = reg.register_mongo_client(
        uri="mongodb://x", client=c1, client_kwargs={"a": 1}
    )
    key2 = reg.register_mongo_client(
        uri="mongodb://x", client=c2, client_kwargs={"a": 1}
    )

    assert key1 == key2
    assert c1.closed is True
    assert c2.closed is False

    st = reg.stats()
    assert len(st["mongo"]) == 1


def test_close_pool_mongo_force_when_leased() -> None:
    reg = ConnectionPoolRegistry()
    key, _ = reg.get_mongo_client(uri="mongodb://localhost:27017")
    reg.lease_mongo(key)

    assert reg.close_pool(key) is False

    assert reg.close_pool(key, force=True) is True
    assert key.dsn not in reg.stats()["mongo"]
