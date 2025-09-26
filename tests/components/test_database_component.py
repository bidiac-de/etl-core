from __future__ import annotations

from typing import Any

import dask.dataframe as dd
import pandas as pd
import pytest
from pydantic import ValidationError

import etl_core.components.databases.database as db


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class DummyCreds:
    """Minimal stand-in for etl_core.context.credentials.Credentials."""

    def __init__(self) -> None:
        self.decrypted_password = "pw"

    def get_parameter(self, key: str) -> Any:
        return {
            "user": "u",
            "database": "d",
            "host": "h",
            "port": 1234,
            "pool_max_size": 5,
            "pool_timeout_s": 10,
        }.get(key)


class DummyMapping:
    """Mimics CredentialsMappingContext with resolve_active_credentials()."""

    def __init__(self, creds: DummyCreds | None = None, cid: str = "cid-123") -> None:
        self._creds = creds or DummyCreds()
        self._cid = cid
        self.called = False

    def resolve_active_credentials(self) -> tuple[DummyCreds, str]:
        self.called = True
        return self._creds, self._cid


# Base testable subclass:
# - Allows no input ports to satisfy Component's validator.
# - get_resolved_context reads a class attribute CTX which we set before __init__.
class TestableDB(db.DatabaseComponent):
    ALLOW_NO_INPUTS = True
    CTX: Any = None  # class-level "injection point" for the validator

    def get_resolved_context(self):
        return self.__class__.CTX

    async def process_row(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"ok": True}

    async def process_bulk(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame()

    async def process_bigdata(self, *args: Any, **kwargs: Any) -> dd.DataFrame:
        return dd.from_pandas(pd.DataFrame([{"x": 1}]), npartitions=1)


def _new_db(cls: type[TestableDB], name: str) -> TestableDB:
    # Provide required base fields; ALLOW_NO_INPUTS avoids port declarations.
    return cls(name=name, description="t", comp_type="db_test")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_build_objects_with_none_context() -> None:
    class NoCtx(TestableDB):
        pass

    NoCtx.CTX = None
    # DatabaseComponent validator triggers during __init__, wrapped as ValidationError
    with pytest.raises(ValidationError) as ei:
        _new_db(NoCtx, "x")
    assert "require a context_id referencing a CredentialsMappingContext" in str(
        ei.value
    )


def test_build_objects_with_wrong_context_type() -> None:
    class WrongCtx(TestableDB):
        pass

    WrongCtx.CTX = "not-a-mapping"
    # Here the model_validator raises a TypeError directly (not wrapped)
    with pytest.raises(TypeError) as ei:
        _new_db(WrongCtx, "x")
    assert "context must be a CredentialsMappingContext" in str(ei.value)


def test_build_objects_and_get_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    # Make DummyMapping pass isinstance(ctx, CredentialsMappingContext)
    monkeypatch.setattr(db, "CredentialsMappingContext", DummyMapping, raising=True)

    class GoodCtx(TestableDB):
        pass

    mapping = DummyMapping()
    GoodCtx.CTX = mapping

    d = _new_db(GoodCtx, "ok")

    # After init, credentials should be resolved and cached
    assert d._credentials is mapping._creds
    assert d._cred_id == "cid-123"

    creds_dict = d._get_credentials()
    assert creds_dict == {
        "user": "u",
        "password": "pw",
        "database": "d",
        "host": "h",
        "port": 1234,
        "pool_max_size": 5,
        "pool_timeout_s": 10,
        "__credentials_id__": "cid-123",
    }


def test_get_credentials_resolves_when_cache_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(db, "CredentialsMappingContext", DummyMapping, raising=True)

    class GoodCtx(TestableDB):
        pass

    mapping = DummyMapping()
    GoodCtx.CTX = mapping

    d = _new_db(GoodCtx, "y")
    # Simulate cleared cache; method should trigger resolve_active_credentials()
    d._credentials = None

    out = d._get_credentials()
    assert mapping.called is True
    assert out["user"] == "u"
    assert out["__credentials_id__"] == "cid-123"


@pytest.mark.asyncio
async def test_concrete_overrides_can_raise_not_implemented(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    You cannot instantiate a class lacking abstract methods. To assert the
    "raises NotImplementedError when used" semantics, implement them but raise.
    Also ensure validation passes by providing a valid mapping context.
    """
    monkeypatch.setattr(db, "CredentialsMappingContext", DummyMapping, raising=True)

    class Incomplete(db.DatabaseComponent):
        ALLOW_NO_INPUTS = True

        def get_resolved_context(self):
            return DummyMapping()

        async def process_row(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
            raise NotImplementedError

        async def process_bulk(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
            raise NotImplementedError

        async def process_bigdata(self, *args: Any, **kwargs: Any) -> dd.DataFrame:
            raise NotImplementedError

    inc = Incomplete(name="bad", description="t", comp_type="db_test")

    with pytest.raises(NotImplementedError):
        await inc.process_row({})

    with pytest.raises(NotImplementedError):
        await inc.process_bulk(pd.DataFrame())

    with pytest.raises(NotImplementedError):
        await inc.process_bigdata(
            dd.from_pandas(pd.DataFrame([{"x": 1}]), npartitions=1)
        )
